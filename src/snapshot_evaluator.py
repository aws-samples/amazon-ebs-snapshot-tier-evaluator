"""
EBS Snapshot Tiering Evaluator

Purpose of this code is to analyse EBS snapshots and calculate AWS spend associated with
moving an EBS Snapshot from EBS Standard Tier to EBS Archive Tier storage.

This code closely aligns with the steps for determining the reduction in standard tier
storage costs outlined in the AWS Documentation:

https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/archiving-guidelines.html#archive-guidelines

This is implemented as an AWS Lambda worker function that sits behind SQS and pulls "jobs"
to evaluate. 

Evaluation requires querying a rate limited API, so query results are cached in S3 once retrieved.

To avoid multiple requests to the pricing API, this function expects to receive pricing data in the
payload. This is sourced in the init stage of the overarching evaluation solution. 
"""

import decimal
import json
import logging
import os
import zlib
from datetime import datetime
from enum import Enum
import boto3
import botocore.exceptions


class EvalScenario(Enum):
    """Enum for Snapshot Evaluation Scenario"""
    BOTH = 1
    NEITHER = 2
    BEFORE = 3
    AFTER = 4


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            if obj.is_zero():
                return "0"
            return str(obj)
        return json.JSONEncoder.default(self, obj)


# Setup logging
logger = logging.getLogger()
for libname in ["boto3", "botocore", "urllib3"]:
    logging.getLogger(libname).setLevel(logging.WARNING)

# Detect current region
aws_session = boto3.session.Session()
current_aws_region = aws_session.region_name

# Setup AWS Clients
s3 = boto3.client('s3')
ec2 = boto3.client('ec2')
ebs = boto3.client('ebs')
dynamodb = boto3.client('dynamodb')


def get_snapshot_blocks(snapshot: str):
    """Function to get the snapshot blocks by calling list_snapshot_blocks"""
    try:
        ebs_response = ebs.list_snapshot_blocks(
            SnapshotId=snapshot
        )
        blocks = ebs_response['Blocks']
        while "NextToken" in ebs_response:
            ebs_response = ebs.list_snapshot_blocks(
                SnapshotId=snapshot,
                NextToken=ebs_response["NextToken"])
            blocks.extend(ebs_response["Blocks"])
        # overwrite with complete array (in case of pagination)
        ebs_response["Blocks"] = blocks
        return ebs_response
    # handle the ResourceNotFoundException from the list_snapshot_blocks api call
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(
                "Hit a ResourceNotFoundException exception. Possible that the EBS Snapshot no longer exists.")
            print(e)
            return None
        else:
            print("Error encountered whilst listing snapshot blocks")
            print(e)
            raise
    except:
        print("Error encountered whilst listing snapshot blocks")
        raise


def calculate_approx_full_snapshot_size(number_of_blocks: int,
                                        block_size_bytes: int):
    """Function to calculate the approximate size of the full snapshot"""
    return number_of_blocks * block_size_bytes


def bytes_to_mb(size_in_bytes: int):
    """Function to convert bytes to MB"""
    BYTES_PER_MB = 1048576         # (2^20 = 1024 x 1024 = 1,048,576)
    return decimal.Decimal(size_in_bytes/BYTES_PER_MB)


def bytes_to_gb(size_in_bytes: int):
    """Function to convert bytes to GB"""
    BYTES_PER_GB = 1073741824  # (2^30 = 1024 x 1024 x 1024 = 1,073,741,824)
    return decimal.Decimal(size_in_bytes/BYTES_PER_GB)


def get_source_volume_id(snapshot_id: str):
    """Function to get the volume id from the snashot id"""
    try:
        ec2_response = ec2.describe_snapshots(
            SnapshotIds=[
                snapshot_id,
            ],
        )
        volume_id = ec2_response["Snapshots"][0]["VolumeId"]
        return volume_id
    except ec2.exceptions.ClientError as e:
        print("Error encountered whilst getting volume id from snapshot id")
        print(e)
        return None


def get_volume_snapshots(ebs_volume_id: str):
    """
    Function calls the AWS API and returns all snapshots for the
    EBS Volume supplied
    """
    ec2_response = ec2.describe_snapshots(
        Filters=[
            {
                'Name': 'volume-id',
                'Values': [
                    ebs_volume_id,
                ]
            },
        ],
    )
    snapshots = ec2_response['Snapshots']
    while "NextToken" in ec2_response:
        ec2_response = ec2.describe_snapshots(
            Filters=[
                {
                    'Name': 'volume-id',
                    'Values': [
                        ebs_volume_id,
                    ]
                },
            ],
            NextToken=ec2_response["NextToken"])
        snapshots.extend(ec2_response["Snapshots"])
    # overwrite with complete array (in case of pagination)
    ec2_response["Snapshots"] = snapshots
    return ec2_response


def sort_snapshots_by_created_date(snapshots: list):
    """
    Function to sort all of the snapshots by the creation date.
    Creation Date == StartTime
    """
    sorted_snapshots = sorted(snapshots, key=lambda x: x.get('StartTime'))
    return sorted_snapshots


def get_surrounding_snapshots(list_of_snapshots: list, the_target_snapshot: str):
    """Returns the snapshot obj before and after the target snapshot

    If either the before or after snapshot is not found, returns None.
    """
    snap_before = snap_after = None
    num_of_snapshots = len(list_of_snapshots)

    for index, snapshot in enumerate(list_of_snapshots):
        if snapshot["SnapshotId"] == the_target_snapshot:
            if index > 0:
                snap_before = list_of_snapshots[index - 1]
            if index < (num_of_snapshots - 1):    # i.e. if not the last snapshot
                snap_after = list_of_snapshots[index + 1]
            break
    return snap_before, snap_after


def determine_eval_scenario(snapshot_before, snapshot_after):
    """Contains the logic to identify the snapshot scenario for the target EBS snapshot
    Informs what processing needs to be done. 
    """

    if not snapshot_before and not snapshot_after:
        logger.info(
            'Step 6a - Current Scenario: No surrounding snapshots to consider.')
        return EvalScenario.NEITHER
    elif snapshot_before and snapshot_after:
        logger.info(
            'Step 6a - Current Scenario: Both before and after snapshots to consider.')
        return EvalScenario.BOTH
    elif snapshot_before and not snapshot_after:
        logger.info(
            'Step 6a - Current Scenario: No subsequent snapshots to consider.')
        return EvalScenario.BEFORE
    elif not snapshot_before and snapshot_after:
        logger.info(
            'Step 6a - Current Scenario: No prior snapshots to consider.')
        return EvalScenario.AFTER
    else:
        raise Exception(
            "Encountered an evaluation scenario which isn't currently catered for.")


def compress_json(data):
    """Function to compress the json string"""
    return zlib.compress(json.dumps(data).encode("utf-8"))


def decompress_json(data):
    """Function to decompress the json string"""
    return json.loads(zlib.decompress(data).decode("utf-8"))


def get_cached_changed_blocks(snap1: str, snap2: str):
    """Function to get the cached changed blocks from the cache"""
    logger.info(
        f"Checking Cache for changed blocks between {snap1} and {snap2}")
    s3_bucket = os.environ['S3_BUCKET_NAME']
    s3_base_path = 'cache/'
    cache_key = f"{snap1}_{snap2}.json.zlib"
    try:
        # if file exists in S3
        s3_object = s3.get_object(
            Bucket=s3_bucket, Key=s3_base_path + cache_key)
        logger.info('Cache Hit')
        result = decompress_json(s3_object['Body'].read())
        # Cache only stores the list of blocks, not the expected json structure.
        return {
            "ChangedBlocks": result
        }
    except s3.exceptions.NoSuchKey:
        logger.info('Cache Miss')
        return False
    except Exception as error:
        logger.warning("We hit an error exception during cache check")
        logger.warning(error)
        return False


def store_cached_changed_blocks(snap1: str, snap2: str, changed_blocks: list):
    """Function to store the changed blocks in the cache"""
    logger.info(f"Storing changed blocks between {snap1} and {snap2} in cache")
    cache_key = f"{snap1}_{snap2}.json.zlib"
    s3_bucket = os.environ['S3_BUCKET_NAME']
    s3_base_path = 'cache/'
    # upload json dump of changed_blocks to file in s3 using cache_key
    s3.put_object(
        Bucket=s3_bucket,
        Key=s3_base_path + cache_key,
        Body=compress_json(changed_blocks)
    )

    logger.info(f"Stored changed blocks between {snap1} and {snap2} in cache.")


def get_changed_blocks(snap1: str, snap2: str):
    """Function calles the list_changed_blocks API and returns the response"""

    cached_result = get_cached_changed_blocks(snap1, snap2)
    if cached_result:
        return cached_result
    else:
        # Grab data from the API
        try:
            ebs_response = ebs.list_changed_blocks(
                FirstSnapshotId=snap1,
                SecondSnapshotId=snap2,
            )
            blocks = ebs_response['ChangedBlocks']
            while "NextToken" in ebs_response:
                ebs_response = ebs.list_changed_blocks(
                    FirstSnapshotId=snap1,
                    SecondSnapshotId=snap2,
                    NextToken=ebs_response["NextToken"])
                blocks.extend(ebs_response["ChangedBlocks"])
            # overwrite with complete array (in case of pagination)
            ebs_response["ChangedBlocks"] = blocks

            # strip response of fields (tokens) we don't need
            for block in ebs_response["ChangedBlocks"]:
                if "FirstBlockToken" in block:
                    del block["FirstBlockToken"]
                if "SecondBlockToken" in block:
                    del block["SecondBlockToken"]
            # store the result in the cache
            store_cached_changed_blocks(
                snap1, snap2, changed_blocks=ebs_response["ChangedBlocks"])
            return ebs_response
        except ebs.exceptions.ValidationException as error:
            logger.warning(
                f"WARN - We hit a validation exception - {error.response['Error']['Message']}")
            if "is empty" in error.response['Error']['Message']:
                # Let's handle the empty snapshot edge case
                # Crafting a "no changed blocks" api response to return
                no_changed_blocks_response = {
                    'ChangedBlocks': [],
                    'ExpiryTime': datetime(2022, 1, 1),
                    'VolumeSize': 123,
                    'BlockSize': 123,
                }
                return no_changed_blocks_response
            raise
        except ebs.exceptions.ResourceNotFoundException as error:
            # This is an edge case that could be hit
            # Depends on initial EBS Snapshot filtering or live env changes
            logger.error(
                'ERROR. We were not able to find this snapshot whilst assessing blocks. It is likely not in a completed state (perhaps deleted). Please try again!')
            return None


def main(target_snapshot: str, pricing: dict):
    """This function contains the main script logic flow"""

    # Expects EBS pricing to be supplied from upstream/calling function
    EBS_STD_SNAPSHOT_PRICE_GB_MONTH = pricing['std_tier_snapshot_pricing']
    EBS_ARCHIVE_SNAPSHOT_PRICE_GB_MONTH = pricing['archive_tier_snapshot_pricing']

    logger.info(
        f"Starting Evaluation of target Snaphot Id: {target_snapshot}")

    eval_data = {}
    eval_data["target_snapshot"] = target_snapshot

    # Step 1 - Determine Full Snapshot Size
    logger.info("Step 1 - Determining the full size of the EBS snapshot...")
    snapshot_blocks = get_snapshot_blocks(target_snapshot)

    if snapshot_blocks is None:
        error_msg = f"Unable to find snapshot blocks for snapshot: {target_snapshot}"
        logger.error(error_msg)
        eval_data[
            "error_message"] = error_msg
        eval_data["error_code"] = "SNAPSHOT_BLOCKS_NOT_FOUND"
        return eval_data

    eval_data["source_ebs_volume_size_gb"] = snapshot_blocks['VolumeSize']
    snapshot_block_size_bytes = snapshot_blocks['BlockSize']
    eval_data["snapshot_block_size_bytes"] = snapshot_block_size_bytes
    number_of_blocks = len(snapshot_blocks["Blocks"])
    approx_full_snapshot_size_bytes = calculate_approx_full_snapshot_size(
        number_of_blocks=number_of_blocks,
        block_size_bytes=snapshot_block_size_bytes
    )
    eval_data["approx_full_snapshot_size_bytes"] = approx_full_snapshot_size_bytes

    # Step 2 - Find Source Volume
    logger.info(
        "Step 2 - Identifying the source EBS volume from the EBS snapshot...")
    snapshot_source_volume_id = get_source_volume_id(
        snapshot_id=target_snapshot)

    if snapshot_source_volume_id is None:
        error_msg = f"Error identifying the source volume for snapshot: {target_snapshot}"
        logger.error(error_msg)
        eval_data["error_message"] = error_msg
        eval_data["error_code"] = "SOURCE_VOLUME_NOT_FOUND"
        return eval_data

    eval_data["snapshot_source_volume_id"] = snapshot_source_volume_id

    # Step 3 - Find all of the snapshots created from the source volume
    logger.info("Step 3 - Finding all snapshots of the source EBS volume...")
    all_volume_snapshots = get_volume_snapshots(
        ebs_volume_id=snapshot_source_volume_id)

    # Step 4 - Sort the snapshots
    logger.info("Step 4 - Sorting the snapshots by created date...")
    sorted_snapshots = sort_snapshots_by_created_date(
        snapshots=all_volume_snapshots["Snapshots"])

    # Step 5 - Get surrounding snapshots
    logger.info(
        "Step 5 - Identifying any prior/following (surrounding) snapshots...")
    snapshot_before, snapshot_after = get_surrounding_snapshots(
        list_of_snapshots=sorted_snapshots, the_target_snapshot=target_snapshot)

    if snapshot_before:
        logger.info(
            f"Step 5a - Snapshot Before Target Snapshot: {snapshot_before['SnapshotId']}")
        eval_data["snapshot_before"] = snapshot_before['SnapshotId']
    else:
        logger.info("Step 5a - Snapshot Before Target Snapshot: None")
        eval_data["snapshot_before"] = None

    if snapshot_after:
        logger.info(
            f"Step 5a - Snapshot After Target Snapshot: {snapshot_after['SnapshotId']}")
        eval_data["snapshot_after"] = snapshot_after['SnapshotId']
    else:
        logger.info("Step 5a - Snapshot After Target Snapshot: None")
        eval_data["snapshot_after"] = None

    # Step 6 - Find the unreferenced data in the target snapshot
    logger.info(
        'Step 6 - Finding any unreferenced blocks in the target snapshot...')

    # 4 possible scenarios:
    #      1 - NEITHER - neither before or after snapshots exist - just this one snapshot to consider
    #      2 - BOTH - both before and after snapshots exists - full change block eval route (per doco).
    #      3 - BEFORE - only the before snapshot exists, none after (i.e. target is most likely the most recent snapshot) - target snap includes 1 set of changed blocks
    #      4 - AFTER - only the after snapshot exists, none before (i.e. first snapshot of volume) - target snap includes it's blocks.
    #               if referenced by any later snapshot, then blocks are retained in std storage under archive scenario.

    logger.info('Step 6a - Identifying the current snapshot scenario...')
    current_eval_scenario = determine_eval_scenario(
        snapshot_before, snapshot_after)

    if current_eval_scenario == EvalScenario.NEITHER:
        # We already have the block information gathered for this scenario (just the one snapshot)
        approx_size_target_snapshot_bytes = approx_full_snapshot_size_bytes
        eval_data["approx_size_target_snapshot_bytes"] = approx_size_target_snapshot_bytes

        approx_size_target_snapshot_removed_bytes = approx_size_target_snapshot_bytes
        eval_data["approx_size_target_snapshot_removed_bytes"] = approx_size_target_snapshot_removed_bytes

    if current_eval_scenario == EvalScenario.BOTH:

        logger.info(
            'Step 6b - Getting changed blocks between previous and target snapshots...')
        # We check the blocks changed for both (before>target and target>after) snapshot references.
        changed_blocks_before = get_changed_blocks(
            snap1=snapshot_before["SnapshotId"], snap2=target_snapshot)
        if changed_blocks_before is None:
            error_msg = f"Unable to find snapshot during changed block analysis: {target_snapshot}"
            logger.error(error_msg)
            eval_data["error_message"] = error_msg
            eval_data["error_code"] = "SNAPSHOT_NOT_FOUND"
            return eval_data

        logger.info(
            'Step 6b - Getting changed blocks between target snapshot and subsequent snapshot...')
        changed_blocks_after = get_changed_blocks(
            snap1=target_snapshot, snap2=snapshot_after["SnapshotId"])
        if changed_blocks_after is None:
            error_msg = f"Unable to find snapshot during changed block analysis: {target_snapshot}"
            logger.error(error_msg)
            eval_data["error_message"] = error_msg
            eval_data["error_code"] = "SNAPSHOT_NOT_FOUND"
            return eval_data

        logger.info(
            'Step 7 - Comparing block indexes to identify unreferenced data in target snapshot...')

        # Making a list of all blocks found in the before-to-target changed blocks comparison
        seen_changed_block_index_before = []
        for b in changed_blocks_before["ChangedBlocks"]:
            seen_changed_block_index_before.append(b["BlockIndex"])

        # Making a list of all blocks found in the target-to-after changed blocks comparison
        seen_changed_block_index_after = []
        for b in changed_blocks_after["ChangedBlocks"]:
            seen_changed_block_index_after.append(b["BlockIndex"])

        # Snapshot size is ANY changed blocks in the before-to-target changed blocks comparison
        approx_size_target_snapshot_bytes = len(
            seen_changed_block_index_before) * snapshot_blocks['BlockSize']

        # Not every snapshot would get removed though, so how much would be removed?
        # Using set intersection to find the blocks that are in both comparisons.
        # This would be the blocks that are deleted if this snapshot was removed.
        block_indexes_in_both_comparisons = set(seen_changed_block_index_before).intersection(
            set(seen_changed_block_index_after))

        # Calculate the amount of space that would be saved by moving this snapshot to archive tier
        approx_size_target_snapshot_removed_bytes = len(
            block_indexes_in_both_comparisons) * snapshot_blocks['BlockSize']

        eval_data["approx_size_target_snapshot_bytes"] = approx_size_target_snapshot_bytes
        eval_data["approx_size_target_snapshot_removed_bytes"] = approx_size_target_snapshot_removed_bytes

    if current_eval_scenario == EvalScenario.BEFORE:
        # BEFORE - only the before snapshot exists, none after (i.e. target is most likely the most recent snapshot) - target snap includes 1 set of changed blocks
        logger.info(
            'Step 6b - Getting changed blocks between previous and target snapshots...')
        changed_blocks_before = get_changed_blocks(
            snap1=snapshot_before["SnapshotId"], snap2=target_snapshot)
        if changed_blocks_before is None:
            error_msg = f"Unable to find snapshot during changed block analysis: {target_snapshot}"
            logger.error(error_msg)
            eval_data["error_message"] = error_msg
            eval_data["error_code"] = "SNAPSHOT_NOT_FOUND"
            return eval_data

        logger.info(
            'Step 7 - Changed block delta contains the unreferenced (changed) data in target snapshot...')

        # Snapshot size is ANY changed blocks in the before-to-target changed blocks comparison
        approx_size_target_snapshot_bytes = len(
            changed_blocks_before["ChangedBlocks"]) * snapshot_blocks['BlockSize']

        approx_size_target_snapshot_removed_bytes = approx_size_target_snapshot_bytes

        eval_data["approx_size_target_snapshot_bytes"] = approx_size_target_snapshot_bytes
        eval_data["approx_size_target_snapshot_removed_bytes"] = approx_size_target_snapshot_removed_bytes

    if current_eval_scenario == EvalScenario.AFTER:
        # No prior snapshots = target snapshot does not reference blocks. Has everything.
        # in the next snapshot (after) any blocks not in the changed list must be retained.
        logger.info(
            'Step 6b - Getting changed blocks between target snapshot and subsequent snapshot...')
        changed_blocks_after = get_changed_blocks(
            snap1=target_snapshot, snap2=snapshot_after["SnapshotId"])
        if changed_blocks_after is None:
            error_msg = f"Unable to find snapshot during changed block analysis: {target_snapshot}"
            logger.error(error_msg)
            eval_data["error_message"] = error_msg
            eval_data["error_code"] = "SNAPSHOT_NOT_FOUND"
            return eval_data

        logger.info(
            'Step 7 - Changed block delta contains amount of blocks that would be no longer referenced in target snapshot...')

        # Snapshot size is ALL blocks in the snapshot
        approx_size_target_snapshot_bytes = len(
            snapshot_blocks['Blocks']) * snapshot_blocks['BlockSize']

        # Expected savings = any block indexes that have changed (and thus aren't referenced in other snapshots)
        approx_size_target_snapshot_removed_bytes = len(
            changed_blocks_after["ChangedBlocks"]) * snapshot_blocks['BlockSize']

        eval_data["approx_size_target_snapshot_bytes"] = approx_size_target_snapshot_bytes
        eval_data["approx_size_target_snapshot_removed_bytes"] = approx_size_target_snapshot_removed_bytes

    logger.info('Step 8 - Determining storage costs for this snapshot...')

    logger.info('Step 8a - Determining storage costs - Standard tier...')
    # Calculating 3-month (90 day) costs for comparison
    cost_estimate_target_snapshot_in_std_tier = (
        bytes_to_gb(approx_size_target_snapshot_bytes) * EBS_STD_SNAPSHOT_PRICE_GB_MONTH) * decimal.Decimal("3")  # months

    eval_data["cost_estimate_90days_target_snapshot_in_std_tier"] = cost_estimate_target_snapshot_in_std_tier

    logger.info('Step 8b - Determining storage costs - Archive tier...')
    cost_estimate_target_snapshot_in_archive_tier = (
        bytes_to_gb(approx_full_snapshot_size_bytes) * EBS_ARCHIVE_SNAPSHOT_PRICE_GB_MONTH) * decimal.Decimal("3")  # months

    eval_data["cost_estimate_90days_target_snapshot_in_archive_tier"] = cost_estimate_target_snapshot_in_archive_tier

    logger.info("Snapshot Evaluation Complete")

    return eval_data


def convert_pricing_data_to_decimal(payload: dict):
    """Converts the pricing data to a decimal"""
    payload['pricing_data']['std_tier_snapshot_pricing'] = decimal.Decimal(
        payload['pricing_data']['std_tier_snapshot_pricing'])
    payload['pricing_data']['archive_tier_snapshot_pricing'] = decimal.Decimal(
        payload['pricing_data']['archive_tier_snapshot_pricing'])
    return payload


def update_ddb_table(jobid: str, snapshot_id: str, data: dict):
    """Updates the DynamoDB Eval Results Table with the results"""
    logger.info(
        f"Updating Status of snapshot ({snapshot_id}) in job ({jobid})")
    try:
        dynamodb.update_item(
            TableName=os.environ.get('DDB_EVAL_RESULTS'),
            Key={'JobId': {'S': jobid},
                 'SnapshotId': {'S': snapshot_id}},
            UpdateExpression="SET #data = :data , completed = :completed",
            ExpressionAttributeNames={'#data': 'data'},
            ExpressionAttributeValues={
                ':data': {'S': json.dumps(data, cls=DecimalEncoder)},
                ':completed': {'S': 'true'}
            }
        )
    except botocore.exceptions.ClientError as e:
        logger.error(
            f"Error updating DynamoDB Item: {e.response['Error']['Message']}")
        raise


def lambda_handler(event, context):
    """Handles invocation as an AWS Lambda function"""
    # Logger Config
    if os.environ.get('LOG_LEVEL') == "DEBUG":
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Perform main business logic
    for record in event['Records']:
        # payload example
        # {
        #     "jobid": jobid,
        #     "snapshot_id": snapshot_id,
        #     "ddb_item_id": f"{jobid}-{snapshot_id}",
        #     "pricing_data": {
        #          'std_tier_snapshot_pricing': std_tier_snapshot_pricing,
        #          'archive_tier_snapshot_pricing': archive_tier_snapshot_pricing
        #      }
        # }
        payload = json.loads(record['body'])
        payload = convert_pricing_data_to_decimal(payload)
        data = main(target_snapshot=payload['snapshot_id'],
                    pricing=payload['pricing_data'])
        update_ddb_table(
            jobid=payload['jobid'], snapshot_id=payload['snapshot_id'], data=data)
        logger.info("Snapshot Evaluation Complete")
        return True
