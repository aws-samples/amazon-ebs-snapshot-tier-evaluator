import * as fs from 'fs'
import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';

export class EBSSnapshotEvalStack extends cdk.Stack {
    constructor(app: cdk.App, id: string) {
        super(app, id);

        const _CONCURRENT_SNAPSHOT_EVALUATIONS = 20;

        //  ======= S3 Bucket =======  //

        const outputBucket = new s3.Bucket(this, 'SnapshotEvalOutput', {
            removalPolicy: cdk.RemovalPolicy.RETAIN,
            encryption: s3.BucketEncryption.S3_MANAGED,
            blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
            versioned: true,
          });

        //  ======= Lambda Functions =======  //

        const getSnapshotsLambda = new lambda.Function(this, 'GetSnapshots', {
            code: new lambda.InlineCode(fs.readFileSync('src/get_snapshot_list.py', { encoding: 'utf-8' })),
            handler: 'index.lambda_handler',
            timeout: cdk.Duration.seconds(120),
            runtime: lambda.Runtime.PYTHON_3_9,
        });

        const getSnapshotsPolicy = new iam.PolicyStatement({
            actions: [
                "ec2:DescribeSnapshots",
            ],
            resources: ['*'],
        });

        getSnapshotsLambda.role?.attachInlinePolicy(
            new iam.Policy(this, 'get-snapshots-policy', {
                statements: [
                    getSnapshotsPolicy
                ],
            }),
        );

        // Snapshot Evaluation Lambda Function

        const evalSnapshotLambda = new lambda.Function(this, 'EvalSnapshots', {
            code: new lambda.InlineCode(fs.readFileSync('src/snapshot_evaluator.py', { encoding: 'utf-8' })),
            handler: 'index.lambda_handler',
            timeout: cdk.Duration.minutes(10),
            runtime: lambda.Runtime.PYTHON_3_9,
            memorySize: 1024,
            environment: {
                'LOG_LEVEL': "INFO",
            },
        });

        const listSnapshotsPolicy = new iam.PolicyStatement({
            actions: [
                "ec2:DescribeSnapshots",
                "ebs:ListSnapshotBlocks",
                "ebs:ListChangedBlocks"
            ],
            resources: ['*'],
        });

        evalSnapshotLambda.role?.attachInlinePolicy(
            new iam.Policy(this, 'list-snapshots-policy', {
                statements: [
                    listSnapshotsPolicy
                ],
            }),
        );

        const pricingApiPolicy = new iam.PolicyStatement({
            actions: [
                "pricing:DescribeServices",
                "pricing:GetAttributeValues",
                "pricing:GetProducts"
            ],
            resources: ['*'],
        });

        evalSnapshotLambda.role?.attachInlinePolicy(
            new iam.Policy(this, 'pricing-api-policy', {
                statements: [
                    pricingApiPolicy
                ],
            }),
        );

        // Output Lambda Function

        const outputSnapshotLambda = new lambda.Function(this, 'OutputSnapshots', {
            code: new lambda.InlineCode(fs.readFileSync('src/results_to_s3.py', { encoding: 'utf-8' })),
            handler: 'index.lambda_handler',
            timeout: cdk.Duration.minutes(2),
            runtime: lambda.Runtime.PYTHON_3_9,
            memorySize: 1024,         
            environment: {
                'LOG_LEVEL': "INFO",
                'S3_BUCKET_NAME': outputBucket.bucketName,
            },
        });
        
        outputBucket.grantWrite(outputSnapshotLambda);

        //  ======= Step Function =======  //

        const getSnapshotsTask = new tasks.LambdaInvoke(this, 'Get Snapshots', {
            lambdaFunction: getSnapshotsLambda,
            outputPath: '$.Payload',
        });

        const evaluateSnapshotMapJob = new sfn.Map(this, 'Map State', {
            maxConcurrency: _CONCURRENT_SNAPSHOT_EVALUATIONS,
            itemsPath: sfn.JsonPath.stringAt('$'),
        });

        const evalSnapshotTask = new tasks.LambdaInvoke(this, 'Evaluate Snapshots', {
            lambdaFunction: evalSnapshotLambda,
            outputPath: '$.Payload',
        });

        evaluateSnapshotMapJob.iterator(evalSnapshotTask);

        const outputTask = new tasks.LambdaInvoke(this, 'Output Snapshots Eval Results', {
            lambdaFunction: outputSnapshotLambda,
            outputPath: '$.Payload',
        });

        // Create chain
        const definition = getSnapshotsTask
            .next(evaluateSnapshotMapJob)
            .next(outputTask)

        // Create state machine
        const stateMachine = new sfn.StateMachine(this, 'SnapshotEvalStateMachine', {
            definition,
        });

    }
}

const app = new cdk.App();
new EBSSnapshotEvalStack(app, 'ebs-snapshot-eval');
app.synth();