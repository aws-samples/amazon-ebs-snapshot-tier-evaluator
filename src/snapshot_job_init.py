import decimal
import json
import os
import re

import boto3

from datetime import datetime

# Detect current region
aws_session = boto3.session.Session()
current_aws_region = aws_session.region_name

# Setup AWS Clients
pricingapi = boto3.client('pricing', region_name='us-east-1')


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            if obj.is_zero():
                return "0"
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def list_snapshots(event):
    """
      Function to get a list of EBS Snapshots. By default, will filter for completed snapshots
      in the standard storage tier - owned by the current account.
    """
    client = boto3.client('ec2', region_name=current_aws_region)
    snapshot_ids = []
    default_snapshot_filter = [
        {
            'Name': 'storage-tier',
            'Values': [
                    'standard'
            ]
        },
        {
            'Name': 'status',
            'Values': [
                    'completed'
            ]
        }
    ]

    print('Describing EBS Snapshots')
    # Support a custom filter object being passed into step function.
    if 'snapshot_filter' in event:
        snapshot_filter = event["snapshot_filter"]
    else:
        snapshot_filter = default_snapshot_filter

    page_iterator = client.get_paginator('describe_snapshots').paginate(
        OwnerIds=['self'],
        Filters=snapshot_filter
    )
    for page in page_iterator:
        for snapshot in page['Snapshots']:
            snapshot_ids.append(snapshot['SnapshotId'])
    return snapshot_ids


def get_std_tier_snapshot_pricing():
    """Function retrieves the price for EBS Standard Tier snapshot storage in the region."""
    response = pricingapi.get_products(
        ServiceCode='AmazonEC2',
        Filters=[
            {
                "Type": "TERM_MATCH",
                "Field": "productFamily",
                "Value": "Storage Snapshot"
            },
            {
                "Type": "TERM_MATCH",
                "Field": "storageMedia",
                "Value": "Amazon S3"
            },
            # {  # This filter works in us-east-1, but as we cannot do wildcards in the Filter here (api doesn't support) we need to filter returned products post this call.
            #     "Type": "TERM_MATCH",
            #     "Field": "usagetype",
            #     "Value": "EBS:SnapshotUsage"
            # },
            {
                "Type": "TERM_MATCH",
                "Field": "regionCode",
                "Value": current_aws_region
            }
        ],
        FormatVersion='aws_v1',
        MaxResults=50,
    )

    for item in response["PriceList"]:
        price_dict = json.loads(item)
        if re.search('.*EBS:SnapshotUsage$', price_dict["product"]["attributes"]["usagetype"]):
            on_demand = price_dict["terms"]["OnDemand"]
            on_demand_key = list(on_demand.values())[0]
            price_dimension = on_demand_key["priceDimensions"]
            price_dimension_key = list(price_dimension.values())[0]
            price_description = price_dimension_key["description"]
            price_per_unit = price_dimension_key["pricePerUnit"]["USD"]
            print(f"Identified Standard Tier Pricing: {price_description}")
            return decimal.Decimal(price_per_unit)

    raise Exception(
        'EBS Standard Storage Price not returned in Pricing API Response')


def get_archive_tier_snapshot_pricing():
    """Function retrieves the price for EBS Archive tier snapshot storage in the region."""
    response = pricingapi.get_products(
        ServiceCode='AmazonEC2',
        Filters=[
            {
                "Type": "TERM_MATCH",
                "Field": "snapshotarchivefeetype",
                "Value": "SnapshotArchiveStorage"
            },
            {
                "Type": "TERM_MATCH",
                "Field": "regionCode",
                "Value": current_aws_region
            }
        ],
        FormatVersion='aws_v1',
        MaxResults=50,
    )

    product_price = response["PriceList"][0]
    price_dict = json.loads(product_price)
    on_demand = price_dict["terms"]["OnDemand"]
    on_demand_key = list(on_demand.values())[0]
    price_dimension = on_demand_key["priceDimensions"]
    price_dimension_key = list(price_dimension.values())[0]
    price_description = price_dimension_key["description"]
    price_per_unit = price_dimension_key["pricePerUnit"]["USD"]
    print(f"Identified Archive Tier Pricing: {price_description}")
    return decimal.Decimal(price_per_unit)


def get_pricing_data():
    """Retrieve data from the AWS Pricing API and return it to the caller."""

    print('Retrieving Pricing Data')

    # Get pricing data
    std_tier_snapshot_pricing = get_std_tier_snapshot_pricing()
    archive_tier_snapshot_pricing = get_archive_tier_snapshot_pricing()

    # Return data
    return {
        'std_tier_snapshot_pricing': std_tier_snapshot_pricing,
        'archive_tier_snapshot_pricing': archive_tier_snapshot_pricing
    }


def get_current_statemachine_execition_name(event):
    """
    Function to get the name (id) of the current state machine execution.
    This will form our jobid as it's unique within the state machine context.
    """
    execution_name = event['Context']['Execution']['Name']
    return execution_name


def lambda_handler(event, context):

    jobid = get_current_statemachine_execition_name(event)

    # Get snapshot data
    snapshot_ids = list_snapshots(event)

    # Get pricing data
    pricing_data = get_pricing_data()

    # Get DynamoDB table names from env variables
    snapshot_job_tracking_table = os.environ['DDB_JOB_TRACKING']
    snapshot_eval_results_table = os.environ['DDB_EVAL_RESULTS']

    # Put record into snapshot job tracking DynamoDB table
    dynamodb = boto3.resource('dynamodb', region_name=current_aws_region)
    table = dynamodb.Table(snapshot_job_tracking_table)
    table.put_item(Item={
        "SnapshotJobId": jobid,
        "DateStarted": str(datetime.now()),
    })

    # Put record into snapshot eval results DynamoDB table
    table = dynamodb.Table(snapshot_eval_results_table)
    for snapshot_id in snapshot_ids:
        ddb_item = {
            "JobId": jobid,
            "SnapshotId": snapshot_id,
            "completed": "false"
        }
        table.put_item(Item=ddb_item)

    # Get SQS Queue URL from env variable
    sqs_queue_url = os.environ['SQS_QUEUE_URL']

    # Push all snapshot IDs to SQS queue
    sqs = boto3.client('sqs', region_name=current_aws_region)
    for snapshot_id in snapshot_ids:
        message = {
            "jobid": jobid,
            "snapshot_id": snapshot_id,
            "ddb_item_id": f"{jobid}-{snapshot_id}",
            "pricing_data": pricing_data
        }
        sqs.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody=json.dumps(message, cls=DecimalEncoder)
        )

    return {
        "jobid": jobid
    }
