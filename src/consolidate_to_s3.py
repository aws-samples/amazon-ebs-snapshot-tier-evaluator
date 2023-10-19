import io
import os
import csv
import json
import time
import boto3


def lambda_handler(event, context):
    """Function to pull results based on a `jobid` from the Snapshot Evalution Results in DynamoDB and output to S3."""
    ddb_table = os.environ.get('DDB_EVAL_RESULTS')

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(ddb_table)

    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key(
            'JobId').eq(event['jobid'])
    )

    s3_bucket = os.environ.get('S3_BUCKET_NAME')
    s3_object_name = f"ebs_snapshot_evaluation_{time.strftime('%Y%m%d%H%M%S')}.csv"
    s3_error_object_name = f"ebs_snapshot_evaluation_errors_{time.strftime('%Y%m%d%H%M%S')}.csv"

    stream = io.StringIO()
    error_stream = io.StringIO()

    data_headers = [
        'target_snapshot',
        'source_ebs_volume_size_gb',
        'snapshot_block_size_bytes',
        'approx_full_snapshot_size_bytes',
        'snapshot_source_volume_id',
        'snapshot_before',
        'snapshot_after',
        'approx_size_target_snapshot_bytes',
        'cost_estimate_90days_target_snapshot_in_std_tier',
        'cost_estimate_90days_target_snapshot_in_archive_tier'
    ]

    writer = csv.DictWriter(stream, fieldnames=data_headers)
    writer.writeheader()

    error_writer = csv.DictWriter(error_stream, fieldnames=[
                                  'JobId', 'SnapshotId', 'ErrorMessage'])
    error_writer.writeheader()

    for item in response['Items']:
        if 'data' not in item:
            print('No data was captured for snapshot {}. Adding to errors output.'.format(
                item['SnapshotId']))
            error_writer.writerow(
                {'JobId': item['JobId'], 'SnapshotId': item['SnapshotId'], 'ErrorMessage': 'No data was captured for this snapshot.'})
            continue

        try:
            eval_result = json.loads(item['data'])

            if 'error_message' in eval_result:
                print('Error Message found for snapshot {}. Adding to errors output.'.format(
                    item['SnapshotId']))
                error_writer.writerow(
                    {'JobId': item['JobId'], 'SnapshotId': item['SnapshotId'], 'ErrorMessage': '{}: {}'.format(eval_result['error_code'], eval_result['error_message'])})
                continue

        except Exception as e:
            print('Error Encountered', e)
            error_writer.writerow(
                {'JobId': item['JobId'], 'SnapshotId': item['SnapshotId'], 'ErrorMessage': 'invalid data format'})
            continue

        writer.writerow(eval_result)

    csv_string_object = stream.getvalue()
    error_csv_string = error_stream.getvalue()

    session = boto3.session.Session()
    s3 = session.resource('s3')

    s3.Object(s3_bucket, s3_object_name).put(Body=csv_string_object)
    s3.Object(s3_bucket, s3_error_object_name).put(Body=error_csv_string)

    return {"output_location": f"s3://{s3_bucket}/{s3_object_name}", "errors_location": f"s3://{s3_bucket}/{s3_error_object_name}"}
