import { App, Aspects } from 'aws-cdk-lib';
import { EBSSnapshotEvalStack } from '../index';
import { AwsSolutionsChecks } from 'cdk-nag';

const app = new App();
new EBSSnapshotEvalStack(app, 'CdkTest');
// Simple rule informational messages
Aspects.of(app).add(new AwsSolutionsChecks());
