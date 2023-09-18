"""
 Lambda function to query DynamoDB table and check status of processing jobs.
 This function is the evaluation step for a Step Function wait loop.

 If all jobs return as complete, it will return a "completed": "true" response.
 Else it will return a "completed": "false" response with the number of jobs still pending.
"""
import os
import boto3
from boto3.dynamodb.conditions import Key


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    ddb_table_name = os.environ['DDB_EVAL_RESULTS']
    table = dynamodb.Table(ddb_table_name)

    # query table where primary key starts with the jobid
    # and where the completed field is false
    # ProjectionExpression used to not needlessly return larger result set
    response = table.query(
        KeyConditionExpression=Key('JobId').eq(event['jobid']),
        FilterExpression=Key('completed').eq('false'),
        ProjectionExpression="JobId,SnapshotId,completed"
    )
    data = response['Items']

    # handle pagination from ddb query
    while 'LastEvaluatedKey' in response:
        response = table.query(
            KeyConditionExpression=Key('JobId').eq(event['jobid']),
            FilterExpression=Key('completed').eq('false'),
            ProjectionExpression="JobId,SnapshotId,completed",
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])

    # if the query returns results, we still have processing to do
    if len(data) > 0:
        num_jobs_pending = len(data)
        print(f"Still processing. {num_jobs_pending} jobs pending")
        return {
            "jobid": event['jobid'],
            "num_jobs_pending": num_jobs_pending,
            "completed": "false"
        }
    else:
        print("Processing complete")
        return {
            "jobid": event['jobid'],
            "completed": "true"
        }
