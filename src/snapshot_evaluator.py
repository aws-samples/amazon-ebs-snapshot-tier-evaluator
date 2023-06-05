"""
!! EBS Snapshot Tiering Evaluator !!

Purpose of this code is to help evaulate AWS spend associated with moving an EBS Snapshot 
from EBS Standard Tier to EBS Archive Tier storage. This code closely aligns with the 
steps for determining the reduction in standard tier storage costs outlined in the AWS 
Documentation: 

https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/archiving-guidelines.html#archive-guidelines
"""

import argparse
import decimal
import json
import logging
import os
import re
import sys
from datetime import datetime
from enum import Enum
import boto3


class EvalScenario(Enum):
    """Enum for Snapshot Evaluation Scenario"""
    BOTH = 1
    NEITHER = 2
    BEFORE = 3
    AFTER = 4


# Setup logging
logger = logging.getLogger()
logger_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(logger_handler)
for libname in ["boto3", "botocore", "urllib3"]:
    logging.getLogger(libname).setLevel(logging.WARNING)

# Detect current region
aws_session = boto3.session.Session()
current_aws_region = aws_session.region_name

# Setup AWS Clients
ec2 = boto3.client('ec2')
ebs = boto3.client('ebs')
pricingapi = boto3.client('pricing', region_name='us-east-1')

# AWS Pricing API Lookups


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
    except:
        print("Error encountered whilst listing snapshot blocks")
        raise


def get_max_block_index(blocks):
    """Function to get the max BlockIndex for the snapshot"""
    max_block_index = int()
    for b in blocks:
        if b["BlockIndex"] > max_block_index:
            max_block_index = b["BlockIndex"]
    return max_block_index


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
    ec2_response = ec2.describe_snapshots(
        SnapshotIds=[
            snapshot_id,
        ],
    )
    volume_id = ec2_response["Snapshots"][0]["VolumeId"]
    return volume_id


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


def get_changed_blocks(snap1: str, snap2: str):
    """Function calles the list_changed_blocks API and returns the response"""
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
        return ebs_response
    except ebs.exceptions.ValidationException as error:
        logger.warning(
            f"WARN - We hit a validation exception - {error.response['Error']['Message']}")
        if "is empty" in error.response['Error']['Message']:
            # Let's handle the empty snapshot edge case and craft an no changed blocks api response and return it.
            no_changed_blocks_response = {
                'ChangedBlocks': [],
                'ExpiryTime': datetime(2022, 1, 1),
                'VolumeSize': 123,
                'BlockSize': 123,
            }
            return no_changed_blocks_response
        raise
    except ebs.exceptions.ResourceNotFoundException as error:
        logger.error(
            'It seems like we were not able to find this snapshot. It is likely not in a completed state. Please try again!')
        raise error


def main(target_snapshot: str, ebs, ec2):
    """This function contains the main script logic flow"""
    print("Looking up region specific EBS snapshot pricing")
    EBS_STD_SNAPSHOT_PRICE_GB_MONTH = get_std_tier_snapshot_pricing()
    EBS_ARCHIVE_SNAPSHOT_PRICE_GB_MONTH = get_archive_tier_snapshot_pricing()

    logger.info(
        f"Starting Evaluation of target Snaphot Id: {target_snapshot}")

    eval_data = {}
    eval_data["target_snapshot"] = target_snapshot

    # Step 1 - Determine Full Snapshot Size
    logger.info("Step 1 - Determining the full size of the EBS snapshot...")
    snapshot_blocks = get_snapshot_blocks(target_snapshot)
    eval_data["source_ebs_volume_size_gb"] = snapshot_blocks['VolumeSize']
    snapshot_block_size_bytes = snapshot_blocks['BlockSize']
    eval_data["snapshot_block_size_bytes"] = snapshot_block_size_bytes
    max_block_index = get_max_block_index(snapshot_blocks["Blocks"])
    approx_full_snapshot_size_bytes = calculate_approx_full_snapshot_size(
        number_of_blocks=max_block_index,
        block_size_bytes=snapshot_block_size_bytes
    )
    eval_data["approx_full_snapshot_size_bytes"] = approx_full_snapshot_size_bytes

    # Step 2 - Find Source Volume
    logger.info(
        "Step 2 - Identifying the source EBS volume from the EBS snapshot...")
    snapshot_source_volume_id = get_source_volume_id(
        snapshot_id=target_snapshot)
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
        eval_data["approx_size_target_snapshot_bytes"] = approx_full_snapshot_size_bytes

    if current_eval_scenario == EvalScenario.BOTH:

        logger.info(
            'Step 6b - Getting changed blocks between previous and target snapshots...')
        # We check the blocks changed for both (before>target and target>after) snapshot references.
        changed_blocks_before = get_changed_blocks(
            snap1=snapshot_before["SnapshotId"], snap2=target_snapshot)

        logger.info(
            'Step 6b - Getting changed blocks between target snapshot and subsequent snapshot...')
        changed_blocks_after = get_changed_blocks(
            snap1=target_snapshot, snap2=snapshot_after["SnapshotId"])

        logger.info(
            'Step 7 - Comparing block indexes to identify unreferenced data in target snapshot...')
        # Making a quick list of all blocks found in the before-to-target changed blocks comparison
        seen_changed_block_index_before = []
        for b in changed_blocks_before["ChangedBlocks"]:
            seen_changed_block_index_before.append(b["BlockIndex"])

        # Now we loop the target-to-after results and look for duplicates in seen_changed_block_index_before
        block_indexes_in_both_comparisons = []
        for b in changed_blocks_after["ChangedBlocks"]:
            if b["BlockIndex"] in seen_changed_block_index_before:
                block_indexes_in_both_comparisons.append(b["BlockIndex"])

        # Calculate the amount of space that would be saved by moving this snapshot to archive tier
        approx_size_target_snapshot_bytes = len(
            block_indexes_in_both_comparisons) * snapshot_blocks['BlockSize']

        eval_data["approx_size_target_snapshot_bytes"] = approx_full_snapshot_size_bytes

    if current_eval_scenario == EvalScenario.BEFORE:
        # BEFORE - only the before snapshot exists, none after (i.e. target is most likely the most recent snapshot) - target snap includes 1 set of changed blocks
        logger.info(
            'Step 6b - Getting changed blocks between previous and target snapshots...')
        changed_blocks_before = get_changed_blocks(
            snap1=snapshot_before["SnapshotId"], snap2=target_snapshot)

        logger.info(
            'Step 7 - Changed block delta contains the unreferenced (changed) data in target snapshot...')

        approx_size_target_snapshot_bytes = len(
            changed_blocks_before["ChangedBlocks"]) * snapshot_blocks['BlockSize']

        eval_data["approx_size_target_snapshot_bytes"] = approx_full_snapshot_size_bytes

    if current_eval_scenario == EvalScenario.AFTER:
        # No prior snapshots = target snapshot does not reference blocks. Has everything.
        # in the next snapshot (after) any blocks not in the changed list must be retained.
        logger.info(
            'Step 6b - Getting changed blocks between target snapshot and subsequent snapshot...')
        changed_blocks_after = get_changed_blocks(
            snap1=target_snapshot, snap2=snapshot_after["SnapshotId"])

        logger.info(
            'Step 7 - Changed block delta contains amount of blocks that would be no longer referenced in target snapshot...')
        # Expected savings = any block indexes that have changed (and thus aren't referenced)

        approx_size_target_snapshot_bytes = len(
            changed_blocks_after["ChangedBlocks"]) * snapshot_blocks['BlockSize']

        eval_data["approx_size_target_snapshot_bytes"] = approx_full_snapshot_size_bytes

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


def display_cli_summary_report(eval_results: dict):
    """Displays the results summary for CLI invocations"""
    # Display Summary Report
    print('')
    logger.info("===== Snapshot Evaulation Report =====")
    logger.info(f"Target Snapshot Id: {eval_results['target_snapshot']}")
    logger.info(
        f"Source EBS Volume ID: {eval_results['snapshot_source_volume_id']}")
    logger.info(
        f"EBS Volume Size: {eval_results['source_ebs_volume_size_gb']} GB")

    # first try to display as GB (if enough blocks)
    if bytes_to_gb(eval_results['approx_size_target_snapshot_bytes']) > 1:
        logger.info(
            f"Approx. size of target snapshot: {bytes_to_gb(eval_results['approx_size_target_snapshot_bytes'])} GB")
    # next display as MB
    elif bytes_to_mb(eval_results['approx_size_target_snapshot_bytes']) > 1:
        logger.info(
            f"Approx. size of target snapshot: {bytes_to_mb(eval_results['approx_size_target_snapshot_bytes'])} MB")
    else:  # display as bytes
        logger.info(
            f"Approx. size of target snapshot: {eval_results['approx_size_target_snapshot_bytes']} bytes")

    logger.info(
        f"Approx. size of full snapshot (if moved to Archive Tier): {round(bytes_to_gb(eval_results['approx_full_snapshot_size_bytes']))} GB")

    print('')
    logger.info(
        f"Estimated 90-day cost of snapshot in Standard Tier (USD): ${round(eval_results['cost_estimate_target_snapshot_in_std_tier'], 2)}")
    logger.info(
        f"Estimated 90-day cost of snapshot in Archive Tier (USD): ${round(eval_results['cost_estimate_target_snapshot_in_archive_tier'], 2)}")
    logger.info("===== End Evaulation Report =====")


def lambda_handler(event, context):
    """Handles invocation as an AWS Lambda function"""
    # Logger Config
    if os.environ.get('LOG_LEVEL') == "DEBUG":
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Perform main business logic
    data = main(target_snapshot=event['target_snapshot'], ebs=ebs, ec2=ec2)
    return data


if __name__ == "__main__":
    """Handles direct python invocations - CLI Script Mode"""
    # Setup command line args / help
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--profile', dest='profile',
                        type=str, help='AWS Named Profile')
    parser.add_argument('-r', '--region', dest='region',
                        type=str, help='AWS Region (e.g. "us-east-1")')
    parser.add_argument('-s', '--snapshot', dest='target_snapshot', type=str, required=True,
                        help='Target Snapshot ID for Evaluation')
    parser.add_argument('-v', '--verbose', dest='verbose', action="store_true",
                        help='(Optional) Display verbose logging (default: false)')
    args = parser.parse_args()

    # Logger Config
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Setup AWS Session
    session_args = {}
    if args.profile is not None:
        session_args['profile_name'] = args.profile
        logger.info(f"AWS Profile: {args.profile}")
    if args.region is not None:
        session_args['region_name'] = args.region
        logger.info(f"Target Region: {args.region}")
    session = boto3.Session(**session_args)

    # Setup AWS Clients
    ec2 = session.client('ec2')
    ebs = session.client('ebs')

    # Perform main business logic
    data = main(target_snapshot=args.target_snapshot, ebs=ebs, ec2=ec2)

    # Display the CLI summary report
    display_cli_summary_report(eval_results=data)
