import io
import os
import csv
import time
import boto3

def lambda_handler(event, context):
    """Function to take a JSON input and output to CSV in S3 bucket"""

    # Setup destination S3 bucket and filename
    s3_bucket = os.environ.get('S3_BUCKET_NAME')
    s3_object_name = f"ebs_snapshot_evaluation_{time.strftime('%Y%m%d%H%M%S')}.csv"

    # Establish in memory stream
    stream = io.StringIO()

    # Setup the CSV
    headers = list(event[0].keys())
    writer = csv.DictWriter(stream, fieldnames=headers)
    writer.writeheader()
    writer.writerows(event)

    # Populate the stream with the CSV data
    csv_string_object = stream.getvalue()

    # Establish S3 session
    session = boto3.session.Session()
    resource = session.resource("s3")

    # Push to S3
    resource.Object(s3_bucket, s3_object_name).put(Body=csv_string_object)

    return { "output_location": f"s3://{s3_bucket}/{s3_object_name}" }
