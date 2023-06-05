import boto3


def lambda_handler(event, context):
    """Function to get a list of EBS Snapshots. By default, will filter for completed snapshots in the standard storage tier - owned by the current account."""

    client = boto3.client('ec2')
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
            snapshot_ids.append({
                "target_snapshot": snapshot['SnapshotId']
            }
            )

    return snapshot_ids
