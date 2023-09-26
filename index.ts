import * as fs from "fs";
import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";

export class EBSSnapshotEvalStack extends cdk.Stack {
  constructor(app: cdk.App, id: string) {
    super(app, id);

    //  ======= S3 Bucket =======  //
    const snapshotEvalBucket = new s3.Bucket(this, "SnapshotEvaluation", {
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      versioned: true,
    });

    // Add lifecycle policy to remove all files in cache/ path in s3 bucket after 7 days
    // https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html
    snapshotEvalBucket.addLifecycleRule({
      expiration: cdk.Duration.days(7),
      prefix: "cache/",
      id: "RemoveCacheFiles",
    });

    //  ======= DynamoDB Tables & Associated Permissions =======  //
    // Snapshot Tracking Table
    const snapshotJobTrackingTable = new cdk.aws_dynamodb.Table(
      this,
      "SnapshotEvalJobs",
      {
        partitionKey: {
          name: "SnapshotJobId",
          type: cdk.aws_dynamodb.AttributeType.STRING,
        },
        billingMode: cdk.aws_dynamodb.BillingMode.PAY_PER_REQUEST,
        removalPolicy: cdk.RemovalPolicy.RETAIN,
        pointInTimeRecovery: false,
        tableName: "SnapshotEvalJobs",
      }
    );

    // Snapshot Evaluation Results Table
    const snapshotEvalResultsTable = new cdk.aws_dynamodb.Table(
      this,
      "SnapshotEvalResults",
      {
        partitionKey: {
          name: "JobId",
          type: cdk.aws_dynamodb.AttributeType.STRING,
        },
        sortKey: {
          name: "SnapshotId",
          type: cdk.aws_dynamodb.AttributeType.STRING,
        },
        billingMode: cdk.aws_dynamodb.BillingMode.PAY_PER_REQUEST,
        removalPolicy: cdk.RemovalPolicy.RETAIN,
        pointInTimeRecovery: false,
        tableName: "SnapshotEvalResults",
      }
    );

    //  ======= SQS Queue =======  //
    const snapshotJobQueue = new cdk.aws_sqs.Queue(
      this,
      "SnapshotEvalJobQueue",
      {
        visibilityTimeout: cdk.Duration.seconds(900),
        receiveMessageWaitTime: cdk.Duration.seconds(5),
        queueName: "SnapshotEvalJobQueue",
      }
    );

    //  ======= Initiation Lambda Function & Associated Permissions =======  //
    const snapshotJobInitLambda = new lambda.Function(this, "SnapshotJobInit", {
      code: new lambda.InlineCode(
        fs.readFileSync("src/snapshot_job_init.py", { encoding: "utf-8" })
      ),
      handler: "index.lambda_handler",
      functionName: "SnapshotsJobInit",
      timeout: cdk.Duration.seconds(300),
      memorySize: 1024,
      runtime: lambda.Runtime.PYTHON_3_11,
      logRetention: cdk.aws_logs.RetentionDays.ONE_MONTH,
      environment: {
        LOG_LEVEL: "INFO",
        DDB_JOB_TRACKING: snapshotJobTrackingTable.tableName,
        DDB_EVAL_RESULTS: snapshotEvalResultsTable.tableName,
        SQS_QUEUE_URL: snapshotJobQueue.queueUrl,
      },
    });

    const pricingApiPolicy = new iam.PolicyStatement({
      actions: [
        "pricing:DescribeServices",
        "pricing:GetAttributeValues",
        "pricing:GetProducts",
      ],
      resources: ["*"],
    });

    snapshotJobInitLambda.role?.attachInlinePolicy(
      new iam.Policy(this, "pricing-api-policy", {
        statements: [pricingApiPolicy],
      })
    );

    const getSnapshotsPolicy = new iam.PolicyStatement({
      actions: ["ec2:DescribeSnapshots"],
      resources: ["*"],
    });

    snapshotJobInitLambda.role?.attachInlinePolicy(
      new iam.Policy(this, "get-snapshots-policy", {
        statements: [getSnapshotsPolicy],
      })
    );

    // DynamoDB Access
    snapshotEvalResultsTable.grantFullAccess(snapshotJobInitLambda);
    snapshotJobTrackingTable.grantFullAccess(snapshotJobInitLambda);
    // SQS Access
    snapshotJobQueue.grantSendMessages(snapshotJobInitLambda);

    //  ======= Snapshot Evaluation Lambda Function & Associated Permissions =======  //
    const evalSnapshotLambda = new lambda.Function(this, "EvalSnapshots", {
      code: new lambda.InlineCode(
        fs.readFileSync("src/snapshot_evaluator.py", { encoding: "utf-8" })
      ),
      handler: "index.lambda_handler",
      functionName: "SnapshotsEvaluator",
      timeout: cdk.Duration.minutes(15),
      runtime: lambda.Runtime.PYTHON_3_11,
      logRetention: cdk.aws_logs.RetentionDays.ONE_MONTH,
      memorySize: 4096,
      environment: {
        LOG_LEVEL: "INFO",
        DDB_EVAL_RESULTS: snapshotEvalResultsTable.tableName,
        S3_BUCKET_NAME: snapshotEvalBucket.bucketName,
      },
    });

    const listSnapshotsPolicy = new iam.PolicyStatement({
      actions: [
        "ec2:DescribeSnapshots",
        "ebs:ListSnapshotBlocks",
        "ebs:ListChangedBlocks",
      ],
      resources: ["*"],
    });

    evalSnapshotLambda.role?.attachInlinePolicy(
      new iam.Policy(this, "list-snapshots-policy", {
        statements: [listSnapshotsPolicy],
      })
    );

    // DynamoDB Access
    snapshotEvalResultsTable.grantFullAccess(evalSnapshotLambda);

    // S3 Access
    snapshotEvalBucket.grantReadWrite(evalSnapshotLambda);

    // Consume SQS Messages
    snapshotJobQueue.grantConsumeMessages(evalSnapshotLambda);

    // Event Source Mapping for SQS & Snapshot Eval Lambda
    const sqsEventSource = new lambda.EventSourceMapping(
      this,
      "EvalSnapshotEventSource",
      {
        target: evalSnapshotLambda,
        batchSize: 1,
        eventSourceArn: snapshotJobQueue.queueArn,
        maxConcurrency: 10,
      }
    );

    //  ======= Check Processing Status Lambda Function & Associated Permissions =======  //
    const checkProcessingStatusLambda = new lambda.Function(
      this,
      "CheckProcessingStatus",
      {
        code: new lambda.InlineCode(
          fs.readFileSync("src/processing_status_checker.py", {
            encoding: "utf-8",
          })
        ),
        handler: "index.lambda_handler",
        functionName: "SnapshotsProcessingStatusChecker",
        timeout: cdk.Duration.minutes(1),
        runtime: lambda.Runtime.PYTHON_3_11,
        logRetention: cdk.aws_logs.RetentionDays.ONE_WEEK,
        memorySize: 128,
        environment: {
          DDB_EVAL_RESULTS: snapshotEvalResultsTable.tableName,
        },
      }
    );

    // DynamoDB Access
    snapshotEvalResultsTable.grantReadData(checkProcessingStatusLambda);

    // Result Consolidation & Output Lambda Function
    const consolidateResultsLambda = new lambda.Function(
      this,
      "OutputSnapshots",
      {
        code: new lambda.InlineCode(
          fs.readFileSync("src/consolidate_to_s3.py", { encoding: "utf-8" })
        ),
        handler: "index.lambda_handler",
        functionName: "SnapshotsResultConsolidator",
        timeout: cdk.Duration.minutes(5),
        runtime: lambda.Runtime.PYTHON_3_11,
        memorySize: 1024,
        environment: {
          LOG_LEVEL: "INFO",
          DDB_EVAL_RESULTS: snapshotEvalResultsTable.tableName,
          S3_BUCKET_NAME: snapshotEvalBucket.bucketName,
        },
      }
    );

    snapshotEvalBucket.grantWrite(consolidateResultsLambda);
    // DynamoDB Access
    snapshotEvalResultsTable.grantReadData(consolidateResultsLambda);

    //  ======= Step Function =======  //

    const initJobTask = new tasks.LambdaInvoke(
      this,
      "Init Snapshot Evaluation Job",
      {
        lambdaFunction: snapshotJobInitLambda,
        payload: sfn.TaskInput.fromObject({
          "Input.$": "$",
          "Context.$": "$$",
        }),
        outputPath: "$.Payload",
      }
    );

    const checkProcessingStatusTask = new tasks.LambdaInvoke(
      this,
      "Check Processing Status",
      {
        lambdaFunction: checkProcessingStatusLambda,
        outputPath: "$.Payload",
      }
    );

    const SECONDS_TO_WAIT_BEFORE_STATUS_CHECK = 60;

    const waitState = new sfn.Wait(
      this,
      `Wait ${SECONDS_TO_WAIT_BEFORE_STATUS_CHECK} seconds`,
      {
        time: sfn.WaitTime.duration(
          cdk.Duration.seconds(SECONDS_TO_WAIT_BEFORE_STATUS_CHECK)
        ),
      }
    );

    const consolidateResultsTask = new tasks.LambdaInvoke(
      this,
      "Output Snapshots Eval Results",
      {
        lambdaFunction: consolidateResultsLambda,
        outputPath: "$.Payload",
      }
    );

    // Create chain
    const definition = initJobTask
      .next(waitState)
      .next(checkProcessingStatusTask)
      .next(
        new sfn.Choice(this, "All Evaluations Completed?", {})
          // when processing is completed
          .when(
            sfn.Condition.stringEquals("$.completed", "true"),
            consolidateResultsTask
          )
          .otherwise(waitState)
      );

    // Create state machine
    const stateMachine = new sfn.StateMachine(
      this,
      "SnapshotEvalStateMachine",
      {
        definitionBody: sfn.DefinitionBody.fromChainable(definition),
        stateMachineName: "SnapshotEvaluator",
      }
    );

    new cdk.CfnOutput(this, "StateMachineArn", {
      value: stateMachine.stateMachineArn,
      description: "The ARN of the state machine",
    });

    new cdk.CfnOutput(this, "StateMachineName", {
      value: stateMachine.stateMachineName,
      description: "The name of the state machine",
    });

    new cdk.CfnOutput(this, "SnapshotEvalBucketName", {
      value: snapshotEvalBucket.bucketName,
      description: "The name of the snapshot evaluation bucket",
    });

    new cdk.CfnOutput(this, "Thanks", {
      value:
        "Thanks for deploying this solution. Get started by invoking the state machine!",
      description: "Thanks Message",
    });
  }
}

const app = new cdk.App();
new EBSSnapshotEvalStack(app, "ebs-snapshot-eval");
app.synth();
