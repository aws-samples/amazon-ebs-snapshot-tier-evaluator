import io
import os
import csv
import json
import time
import boto3


def lambda_handler(event, context):
    """Function to take a JSON input and output to CSV in S3 bucket"""

    ddb_table = os.environ.get('DDB_EVAL_RESULTS')
    # Query the Dynamodb table to pull all eval results based on jobid in event
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(ddb_table)
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key(
            'JobId').eq(event['jobid'])
    )

    # Setup destination S3 bucket and filename
    s3_bucket = os.environ.get('S3_BUCKET_NAME')
    s3_object_name = f"ebs_snapshot_evaluation_{time.strftime('%Y%m%d%H%M%S')}.csv"

    # Establish in memory stream
    stream = io.StringIO()

    # Setup the CSV
    # Set headers based on keys from first item
    headers = list(json.loads(response['Items'][0]['data']).keys())
    writer = csv.DictWriter(stream, fieldnames=headers)
    writer.writeheader()

    # loop the eval results
    for item in response['Items']:
        eval_result = json.loads(item['data'])
        print(eval_result)
        writer.writerow(eval_result)

    # Populate the stream with the CSV data
    csv_string_object = stream.getvalue()

    # Establish S3 session
    session = boto3.session.Session()
    resource = session.resource("s3")

    # Push to S3
    resource.Object(s3_bucket, s3_object_name).put(Body=csv_string_object)

    return {"output_location": f"s3://{s3_bucket}/{s3_object_name}"}
