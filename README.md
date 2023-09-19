# Amazon EBS Snapshot Tiering Evaluator

This sample was written to enable "What If" cost analysis associated with moving an Amazon EBS Snapshot from Amazon EBS Standard Tier to Amazon EBS Archive Tier storage. The main output of this solution is the estimated AWS cost assosciated with the target snapshot in both Standard and Archive tiers.

When you archive a snapshot, the incremental snapshot is converted to a full snapshot, and it is moved from the standard tier to the Amazon EBS Snapshots Archive tier (archive tier). Full snapshots include all of the blocks that were written to the volume at the time when the snapshot was created. The variance between incremental and full snapshots means effective up-front cost analysis can become an important factor to consider.

This data is a valuable input to decision making around whether EBS Snapshot Archival makes sense for a particular snapshot or group of snapshots (e.g. is it cost effective). Whilst this is one input, other criteria (such as regulatory and compliance requirements) should be factored in when using Amazon EBS Snapshots Archive for low-cost, long-term storage of snapshots.

This code closely aligns with the steps for determining the reduction in standard tier storage costs outlined in the [AWS Documentation](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/archiving-guidelines.html#archive-guidelines).

## Assumptions & Key Notes

- Pricing for Amazon EBS is based on GB-month.
- This sample works on a 30-day month.
- Snapshot storage within the Archive Tier currently charges a minimum of 90 days. As this solution compares pricing between EBS Standard Tier storage and the Archive Tier, we are using 3 months of storage (3x 30-day months) as our assumption and method of comparison. As a result, estimated costs are for 90 days (not a monthly cost).
- **Please Note:** This script calls Amazon EBS direct APIs for Snapshots which do have costs associated with their usage. Under standard usage, the costs associated with tool is not expected to raise any concerns (`ListChangedBlocks` and `ListSnapshotBlocks` API calls). Please make sure to review your expected usage of this script and the EBS Pricing available at: https://aws.amazon.com/ebs/pricing/
- The `ListChangedBlocks` API is called for evaluation of snapshot blocks and is rate limited. This solution leverages a queue and worker process to work within these API constraints.
- This sample calls the AWS Pricing API in us-east-1 to dynamically source Amazon EBS snapshot storage pricing for the region.

## How to Use

Use this solution to generate a "What If" cost analysis associated with moving an Amazon EBS Snapshot from Amazon EBS Standard Tier to Amazon EBS Archive Tier storage.

1. Build & Deploy the solution.
2. Run the solution to generate the CSV report.
3. Review the CSV.
   - Check out the [common questions](#common-questions) this analysis helps answer.
   - An overview of the CSV structure can be found [here](#example-output).
4. (Optional) Cleanup solution.

When reviewing the results remember that cost estimates are based on storing the snapshot for [90 days](#assumptions--key-notes)!

### Common Questions

Some common questions to aid decision making based on this analysis are listed below:

- Which target snapshots would be cost effective to move to the Archive storage tier?
- Which snapshots would cost more if moved to Archive storage tier? What does the cost look like?
- What would it cost if we did move XXX snapshot to the ARCHIVE tier?
- Which EBS volumes are costing us the most for EBS Snapshot storage?
- Which EBS snapshots contain the most changed blocks? What does the distribution look like?
- If we were to remove X, Y & Z snapshots, what cost impact/savings would that achieve?

## Prerequisites

- Python 3.9+
- AWS Account Credentials
- [AWS Cloud Development Kit (CDK)](https://aws.amazon.com/cdk/)
- Bootstrapped AWS Account (per https://docs.aws.amazon.com/cdk/v2/guide/bootstrapping.html)
- Node.js 18.16.0+

## Get Started (AWS CDK)

Use the following steps to deploy this sample solution into your AWS environment.
The following steps expect that your AWS target environment & associated credentials have been setup for deployment into your desired AWS Account & region.

### Build

To build this app, you need to be in this sample's root folder. Then run the following:

```bash
$ npm install -g aws-cdk
$ npm install
$ npm run build
```

### Deploy

_This deploy step assumes your local environment is setup with AWS Credentials, and that you're targetting a bootstrapped AWS account/region. More details on AWS CDK CLI bootstrapping commands can be found [here](https://docs.aws.amazon.com/cdk/v2/guide/cli.html#cli-bootstrap)._

This solution is deployed using AWS CDK ([cdk deploy](https://docs.aws.amazon.com/cdk/v2/guide/cli.html#cli-deploy)). This will deploy / redeploy the EBS Snapshot Evaluator solution to your AWS Account.

```
cdk deploy
```

### Execution

1. In the AWS Management Console, navigate to the [AWS Step Functions](https://console.aws.amazon.com/states/home) page and select the State Machine you deployed.

   **State Machine Name:** `SnapshotEvaluator`

2. **Start the Execution** of the state machine. State Machine input is not required (to accept defaults). If you would like to filter your snapshots for evaluation, supply a [custom EBS snapshot filter](#custom-ebs-snapshot-filter) as an input.

### Cleanup

This section outlines how to cleanup the resources associated with this solution.

- From the solution directory, run `cdk destroy` to initiate the cleanup of resources.
- The solution is set to retain the following resources. Delete these as needed:
  - The S3 Bucket used for the output CSV file (and cache).
  - 2x DynamoDB tables (SnapshotEvalJobs & SnapshotEvalResults)

## Solution Overview

This solution is orchestrated using an AWS Step Functions state machine. In addition to the state machine, we have a Amazon SQS queue-based processing function to facilitate quick evaluation of EBS Snapshots within the deployed account/region.

<kbd>
<img src="./assets/architecture.jpg" width="500px" margin="auto" />
</kbd>

#### Step 1 - Init Function

The **Init Function** establishes a new job run, obtains the relevant AWS pricing from the AWS Pricing API and also generates a list of EBS Snapshots in scope for this evaluation job.

Be default, this sample will filter for all EBS snapshots which meet the criteria:

- Status = Completed
- Storage Tier = Standard
- Same Account and Region as the Lambda Function
- Owned by the current account

All in scope snapshots are registered in the DynamoDB tables, and submitted to the solution's SQS queue for subsequent processing.

##### Custom EBS Snapshot Filter

You can optionally supply an Amazon EBS Snapshot filter to more selectively target certain snapshots. This section outlines the syntax and expected input that should be supplied in the State Machine Execution Input.

Ensure you encapsulate your custom array of filters in the `snapshot_filter` key. All other key names are ignored for this step.

Supply filters based upon the boto3 format detailed here: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.describe_snapshots

**Please Note:** It is highly recommended you include the `storage-tier` and `status` filters listed in the example below, and append your custom filters past that.

**Pro Tip:** State Machine input wants JSON formatting, this differs from the single-quotes used in the boto3 documentation.

The following JSON example provides a starting point for building your custom snapshot filter.

```json
{
  "snapshot_filter": [
    {
      "Name": "storage-tier",
      "Values": ["standard"]
    },
    {
      "Name": "status",
      "Values": ["completed"]
    },
    {
      "Name": "REPLACE_ME",
      "Values": ["REPLACE_ME"]
    }
  ]
}
```

#### Step 2 - Processing Wait Loop

While processing is underway (outside the scope of the State Machine) the state machine now enters into a wait loop. By default the state machine checks progress every minute.

You can track progress of the snapshot evaluation processing through a few mechanisms:

- The `CheckProcessingStatus` function provides a `num_jobs_pending` metric in its output you can monitor.
- Alternatively, you could watch the SQS Queue depth for any pending jobs.
- Alternatively, you could query the solution's `SnapshotEvalJobs` DynamoDB table which contains the status of all jobs.

The duration this solution stay in this processing wait loop depends on the number of snapshots in scope and the size (number of blocks changed).

##### Out of Band Processing

A Lambda Worker process is seutp behind the SQS Queue (concurrency limited) to process each of the in scope jobs. This job processes each snapshot, queries the EBS Direct APIs and stores results in a cache in S3. There is a 7 day expiry configured for this cache data in Amazon S3.

This worker will:

- Pull job from SQS
- Query the EBS Direct APIs (+ cache)
- Evaluate the snapshot
- Emit results into Amazon DynamoDB tracking table

#### Step 3 - Output Results

Once all processing jobs have reported complete, we move onto the output results stage.

This Lambda function pulls and collate all the job results into a CSV file. This CSV file is pushed into the Snapshot Eval Bucket in Amazon S3. Download this file for your analysis. Of most note is the final few columns (which compare 90-day cost associated with each snapshot in either tier).

**NB** For ease of use, the output step within the step function includes the specific path that the CSV is uploaded to in S3.

##### Example Output

The CSV file contains an unfiltered view of all metadata collected for each snapshot being processed. The following table provides an overview of each column.

| Column Heading                                         | Description                                                                                                                                             |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `target_snapshot`                                      | The EBS snapshot id that was evaluated.                                                                                                                 |
| `source_ebs_volume_size_gb`                            | The size (in Gb) of the source EBS volume.                                                                                                              |
| `snapshot_block_size_bytes`                            | The block size (in bytes) of the EBS snapshot.                                                                                                          |
| `approx_full_snapshot_size_bytes`                      | The approximate size (in bytes) that a _full snapshot_ (all blocks) would be. Key to understanding what costs would be in the Archive tier.             |
| `snapshot_source_volume_id`                            | The source EBS volume id that the target snapshot.                                                                                                      |
| `snapshot_before`                                      | [Backup Chain] The EBS snapshot id of a snapshot immediately before (prior) the target snapshot. This value may be null if no prior snapshots exist.    |
| `snapshot_after`                                       | [Backup Chain] The EBS snapshot id of a snapshot immediately after (post) the target snapshot. This value may be null if no subsequent snapshots exist. |
| `approx_size_target_snapshot_bytes`                    | This column contains the approximate size of the target snapshot (in bytes). Key to understanding what costs within the Standard Tier.                  |
| `cost_estimate_90days_target_snapshot_in_std_tier`     | :star: [Cost Estimate] This column contains the estimated 90 day cost for having the target snapshot in the **STANDARD tier.**                          |
| `cost_estimate_90days_target_snapshot_in_archive_tier` | :star: [Cost Estimate] This column contains the estimated 90 day cost for having the target snapshot in the **ARCHIVE tier.**                           |

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
