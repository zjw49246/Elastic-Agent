# Elastic-Agent IAM least-privilege cutover

This runbook is intentionally scoped to account `297645381734`, Region
`ap-northeast-1`, Manager instance `i-07988886030e168cc`, result bucket
`elastic-agent-results-297645381734`, and Worker role/profile
`elastic-agent-worker`.

Run every mutating command from a separate administrator/control instance, not
from `i-07988886030e168cc`. The new Manager role deliberately cannot replace
its own instance profile or edit IAM.

## Why the existing Manager role must not be tightened

The instance profile `Manager` is currently attached to seven instances:

```text
i-0e0eb4d47c6e3a075  i-03e9984e1c983a1a0  i-06c940fa448e9059c
i-0351fab2f447ebfaf  i-0b1a45f7f632c07f0  i-0f51cc51d16bbda74
i-07988886030e168cc
```

Its role has `AdministratorAccess`, `AmazonEC2FullAccess`, and
`AmazonS3FullAccess`, plus unrelated inline policies. Detaching any of those
from the shared role can break CCM and other machines. Create a dedicated
`elastic-agent-manager` role/profile and replace only the association on
`i-07988886030e168cc`.

The production audit found 65 persisted JobSpecs, zero `setup.s3_datasets`
entries, and zero `run.secret_env` entries. Therefore the Worker policy can be
write-only under `jobs/*` without breaking a historical Job. A future S3
dataset must get an explicit, separately reviewed read statement for its exact
bucket/prefix; do not restore `AmazonS3FullAccess`.

CloudTrail for the target Manager instance observed only these runtime service
families/actions: EC2 instance/EIP describe, create, associate, detach and
terminate; KMS EBS grant/data-key calls; SSM managed-node heartbeats; STS
identity. Manager-side result upload/read uses S3 data-plane calls, which are
not present in default CloudTrail event history. The policy also follows the
actual boto3/AWS CLI calls in this repository. Worker result collection uses
local-to-S3 `aws s3 cp --recursive`, not `sync`, so it does not need
`ListBucket`; removing it also hides other Jobs' object names. The separate
`GetBucketLocation` permission is retained for CLI/region-discovery
compatibility without granting bucket listing or object reads. Object transfer
and failed multipart cleanup map to `PutObject` and `AbortMultipartUpload`.

## 1. Prepare the concrete policy and read-only preflight

Set the final golden AMI and dedicated Worker security group. The deployed
`serve_manager.py` values must match these IDs before the first Job launch.
The IAM policy also intentionally pins `ec2:InstanceType` to `t3.large`, which
matches the production Job allowlist. Before enabling another per-Job instance
type, add it to both allowlists, re-run simulation for that type, and deploy the
two changes together; otherwise `RunInstances` must remain denied.

```bash
export AWS_REGION=ap-northeast-1
export EA_MANAGER_INSTANCE=i-07988886030e168cc
export EA_MANAGER_ROLE=elastic-agent-manager
export EA_MANAGER_PROFILE=elastic-agent-manager
export EA_WORKER_ROLE=elastic-agent-worker
export FINAL_AMI_ID=ami-REPLACE_ME
export FINAL_WORKER_SECURITY_GROUP_ID=sg-05c68220f901fb555

[[ "$FINAL_AMI_ID" =~ ^ami-[0-9a-f]{17}$ ]]
[[ "$FINAL_WORKER_SECURITY_GROUP_ID" =~ ^sg-[0-9a-f]{17}$ ]]

EA_IAM_TMP=$(mktemp -d)
sed \
  -e "s/FINAL_AMI_ID/$FINAL_AMI_ID/g" \
  deploy/aws/elastic-agent-manager-policy.json \
  > "$EA_IAM_TMP/manager-policy.json"
cp deploy/aws/elastic-agent-worker-policy.json "$EA_IAM_TMP/worker-policy.json"
jq -e . "$EA_IAM_TMP/manager-policy.json" "$EA_IAM_TMP/worker-policy.json"
! grep -q 'FINAL_' "$EA_IAM_TMP/manager-policy.json"

# Manager policy is intentionally inline: its compact form is above the
# 6,144-character customer-managed-policy limit but below the 10,240-character
# role-inline-policy limit.
test "$(jq -c . "$EA_IAM_TMP/manager-policy.json" | wc -c)" -le 10240

OLD_ASSOCIATION_ID=$(aws ec2 describe-iam-instance-profile-associations \
  --region "$AWS_REGION" \
  --query "IamInstanceProfileAssociations[?InstanceId=='$EA_MANAGER_INSTANCE'].AssociationId | [0]" \
  --output text)
OLD_PROFILE_ARN=$(aws ec2 describe-iam-instance-profile-associations \
  --region "$AWS_REGION" \
  --association-ids "$OLD_ASSOCIATION_ID" \
  --query 'IamInstanceProfileAssociations[0].IamInstanceProfile.Arn' \
  --output text)
test "$OLD_ASSOCIATION_ID" = iip-assoc-06d09f4c1a54ed6a2
test "$OLD_PROFILE_ARN" = arn:aws:iam::297645381734:instance-profile/Manager

# There must be no live Elastic-Agent Worker while changing either role.
test "$(aws ec2 describe-instances --region "$AWS_REGION" \
  --filters Name=tag:ManagedBy,Values=elastic-agent \
            Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'length(Reservations[].Instances[])' --output text)" = 0

ssh -i ~/.ssh/interview-key.pem -o BatchMode=yes \
  ubuntu@172.31.46.129 \
  'systemctl is-active --quiet elastic-agent-manager.service'
curl --fail --silent --show-error \
  https://elastic-agent.claude-code-manager.com/api/health >/dev/null
```

Do not proceed if a Job, account lease, EIP attachment, Worker bootstrap, or
collection is active even if the EC2 query happens to be empty.

## 2. Validate and simulate before any IAM write

```bash
aws accessanalyzer validate-policy --policy-type IDENTITY_POLICY \
  --policy-document "file://$EA_IAM_TMP/manager-policy.json" \
  | jq -e '.findings | length == 0'
aws accessanalyzer validate-policy --policy-type IDENTITY_POLICY \
  --policy-document "file://$EA_IAM_TMP/worker-policy.json" \
  | jq -e '.findings | length == 0'

MANAGER_POLICY=$(jq -c . "$EA_IAM_TMP/manager-policy.json")
WORKER_POLICY=$(jq -c . "$EA_IAM_TMP/worker-policy.json")

# Worker: result upload is allowed, while listing, cross-prefix writes, object
# reads, and deletes are denied.
test "$(aws iam simulate-custom-policy --policy-input-list "$WORKER_POLICY" \
  --action-names s3:PutObject \
  --resource-arns \
    arn:aws:s3:::elastic-agent-results-297645381734/jobs/job-test/result.json \
  --query 'EvaluationResults[0].EvalDecision' --output text)" = allowed
test "$(aws iam simulate-custom-policy --policy-input-list "$WORKER_POLICY" \
  --action-names s3:PutObject \
  --resource-arns \
    arn:aws:s3:::elastic-agent-results-297645381734/private/result.json \
  --query 'EvaluationResults[0].EvalDecision' --output text)" = implicitDeny
test "$(aws iam simulate-custom-policy --policy-input-list "$WORKER_POLICY" \
  --action-names s3:GetObject s3:DeleteObject \
  --resource-arns \
    arn:aws:s3:::elastic-agent-results-297645381734/jobs/job-test/result.json \
  --query 'length(EvaluationResults[?EvalDecision!=`implicitDeny`])' \
  --output text)" = 0
test "$(aws iam simulate-custom-policy --policy-input-list "$WORKER_POLICY" \
  --action-names s3:ListBucket \
  --resource-arns arn:aws:s3:::elastic-agent-results-297645381734 \
  --context-entries \
    'ContextKeyName=s3:prefix,ContextKeyValues=jobs/job-test/,ContextKeyType=string' \
  --query 'EvaluationResults[0].EvalDecision' --output text)" = implicitDeny

# Manager: only the Worker role can be passed, and only to EC2.
test "$(aws iam simulate-custom-policy --policy-input-list "$MANAGER_POLICY" \
  --action-names iam:PassRole \
  --resource-arns arn:aws:iam::297645381734:role/elastic-agent-worker \
  --context-entries \
    'ContextKeyName=iam:PassedToService,ContextKeyValues=ec2.amazonaws.com,ContextKeyType=string' \
    'ContextKeyName=iam:AssociatedResourceArn,ContextKeyValues=arn:aws:ec2:ap-northeast-1:297645381734:instance/i-0123456789abcdef0,ContextKeyType=string' \
  --query 'EvaluationResults[0].EvalDecision' --output text)" = allowed

# Managed instances can be terminated; an untagged/foreign instance cannot.
test "$(aws iam simulate-custom-policy --policy-input-list "$MANAGER_POLICY" \
  --action-names ec2:TerminateInstances \
  --resource-arns \
    arn:aws:ec2:ap-northeast-1:297645381734:instance/i-0123456789abcdef0 \
  --context-entries \
    'ContextKeyName=aws:RequestedRegion,ContextKeyValues=ap-northeast-1,ContextKeyType=string' \
    'ContextKeyName=ec2:ResourceTag/ManagedBy,ContextKeyValues=elastic-agent,ContextKeyType=string' \
  --query 'EvaluationResults[0].EvalDecision' --output text)" = allowed
test "$(aws iam simulate-custom-policy --policy-input-list "$MANAGER_POLICY" \
  --action-names ec2:TerminateInstances \
  --resource-arns \
    arn:aws:ec2:ap-northeast-1:297645381734:instance/i-0123456789abcdef0 \
  --context-entries \
    'ContextKeyName=aws:RequestedRegion,ContextKeyValues=ap-northeast-1,ContextKeyType=string' \
    'ContextKeyName=ec2:ResourceTag/ManagedBy,ContextKeyValues=foreign,ContextKeyType=string' \
  --query 'EvaluationResults[0].EvalDecision' --output text)" = implicitDeny

# A representative valid launch must be allowed across every resource type.
test "$(aws iam simulate-custom-policy --policy-input-list "$MANAGER_POLICY" \
  --action-names ec2:RunInstances \
  --resource-arns \
    "arn:aws:ec2:ap-northeast-1::image/$FINAL_AMI_ID" \
    arn:aws:ec2:ap-northeast-1:297645381734:subnet/subnet-0c1db80817d054277 \
    arn:aws:ec2:ap-northeast-1:297645381734:security-group/sg-05c68220f901fb555 \
    arn:aws:ec2:ap-northeast-1:297645381734:key-pair/key-0a094b8bdfddeaa38 \
    arn:aws:ec2:ap-northeast-1:297645381734:network-interface/eni-0123456789abcdef0 \
    arn:aws:ec2:ap-northeast-1:297645381734:volume/vol-0123456789abcdef0 \
    arn:aws:ec2:ap-northeast-1:297645381734:instance/i-0123456789abcdef0 \
  --context-entries \
    'ContextKeyName=aws:RequestedRegion,ContextKeyValues=ap-northeast-1,ContextKeyType=string' \
    'ContextKeyName=aws:RequestTag/ManagedBy,ContextKeyValues=elastic-agent,ContextKeyType=string' \
    'ContextKeyName=aws:TagKeys,ContextKeyValues=ManagedBy,Name,ElasticAgentJob,ElasticAgentController,ContextKeyType=stringList' \
    'ContextKeyName=ec2:VolumeType,ContextKeyValues=gp3,ContextKeyType=string' \
    'ContextKeyName=ec2:Encrypted,ContextKeyValues=true,ContextKeyType=boolean' \
    'ContextKeyName=ec2:VolumeSize,ContextKeyValues=100,ContextKeyType=numeric' \
    'ContextKeyName=ec2:InstanceType,ContextKeyValues=t3.large,ContextKeyType=string' \
    'ContextKeyName=ec2:MetadataHttpTokens,ContextKeyValues=required,ContextKeyType=string' \
    'ContextKeyName=ec2:MetadataHttpPutResponseHopLimit,ContextKeyValues=1,ContextKeyType=numeric' \
    'ContextKeyName=ec2:InstanceProfile,ContextKeyValues=arn:aws:iam::297645381734:instance-profile/elastic-agent-worker,ContextKeyType=string' \
  --query 'EvaluationResults[0].EvalDecision' --output text)" = allowed
```

Also change the simulated AMI or instance type and confirm `implicitDeny` before
cutover. Access Analyzer validation and IAM simulation do not replace one real
create/upload/terminate canary.

## 3. Create the dedicated Manager role/profile

These are the first mutating commands. Do not attach the AWS-managed
`AmazonSSMManagedInstanceCore`: it includes broad `ssm:GetParameter(s)` on `*`.
The inline policy carries the required SSM channel/heartbeat actions while only
allowing SSM Job parameters below `/elastic-agent/` and Secrets Manager names
below `elastic-agent/`.

```bash
TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

aws iam create-role \
  --role-name "$EA_MANAGER_ROLE" \
  --description 'Dedicated least-privilege Elastic-Agent Manager runtime' \
  --assume-role-policy-document "$TRUST_POLICY" \
  --tags Key=ManagedBy,Value=elastic-agent
aws iam wait role-exists --role-name "$EA_MANAGER_ROLE"
aws iam put-role-policy \
  --role-name "$EA_MANAGER_ROLE" \
  --policy-name ElasticAgentManagerRuntime \
  --policy-document "file://$EA_IAM_TMP/manager-policy.json"
aws iam create-instance-profile \
  --instance-profile-name "$EA_MANAGER_PROFILE" \
  --tags Key=ManagedBy,Value=elastic-agent
aws iam add-role-to-instance-profile \
  --instance-profile-name "$EA_MANAGER_PROFILE" \
  --role-name "$EA_MANAGER_ROLE"
aws iam wait instance-profile-exists \
  --instance-profile-name "$EA_MANAGER_PROFILE"

# Add the narrow Worker policy while the old S3 policy is still present.
aws iam put-role-policy \
  --role-name "$EA_WORKER_ROLE" \
  --policy-name ElasticAgentWorkerResultsOnly \
  --policy-document "file://$EA_IAM_TMP/worker-policy.json"
```

Read both policies back and compare their canonical JSON hashes with the local
files. Wait for IAM propagation, then run `simulate-principal-policy` against
`arn:aws:iam::297645381734:role/elastic-agent-manager`. Do not use principal
simulation to assess the Worker deny cases yet: `AmazonS3FullAccess` is still
attached and intentionally masks them until the second cutover.

## 4. Replace only the target Manager association

Keep the current administrator SSH session open. Capture the returned new
association ID; it is required for rollback.

```bash
CUTOVER_JSON=$(aws ec2 replace-iam-instance-profile-association \
  --region "$AWS_REGION" \
  --association-id "$OLD_ASSOCIATION_ID" \
  --iam-instance-profile Name="$EA_MANAGER_PROFILE")
NEW_ASSOCIATION_ID=$(jq -r \
  '.IamInstanceProfileAssociation.AssociationId' <<<"$CUTOVER_JSON")
test -n "$NEW_ASSOCIATION_ID"

for attempt in $(seq 1 30); do
  state=$(aws ec2 describe-iam-instance-profile-associations \
    --region "$AWS_REGION" --association-ids "$NEW_ASSOCIATION_ID" \
    --query 'IamInstanceProfileAssociations[0].State' --output text)
  test "$state" = associated && break
  sleep 2
done
test "$state" = associated

# A fresh process on the target must now use only the dedicated role.
for attempt in $(seq 1 30); do
  role_arn=$(ssh -i ~/.ssh/interview-key.pem -o BatchMode=yes \
    ubuntu@172.31.46.129 \
    "aws sts get-caller-identity --query Arn --output text")
  [[ "$role_arn" == arn:aws:sts::297645381734:assumed-role/elastic-agent-manager/* ]] \
    && break
  sleep 2
done
[[ "$role_arn" == arn:aws:sts::297645381734:assumed-role/elastic-agent-manager/* ]]

# Restart to discard boto3 credentials cached by the long-running process.
ssh -i ~/.ssh/interview-key.pem -o BatchMode=yes ubuntu@172.31.46.129 \
  'sudo systemctl restart elastic-agent-manager.service && \
   systemctl is-active --quiet elastic-agent-manager.service'
curl --fail --silent --show-error \
  https://elastic-agent.claude-code-manager.com/api/health >/dev/null
```

Run one ordinary one-Worker canary while `AmazonS3FullAccess` is still attached
to the Worker role. It must launch with the final AMI/SG/profile, upload a file
under `jobs/<job>/workers/shard-00000/results/`, terminate the EC2/root EBS, and
leave zero live managed Workers. This isolates Manager-policy failures from
Worker-S3-policy failures.

## 5. Remove Worker `AmazonS3FullAccess`

Repeat the zero-live-Worker and zero-active-Job checks first.

```bash
aws iam detach-role-policy \
  --role-name "$EA_WORKER_ROLE" \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
aws iam list-attached-role-policies --role-name "$EA_WORKER_ROLE"
aws iam get-role-policy --role-name "$EA_WORKER_ROLE" \
  --policy-name ElasticAgentWorkerResultsOnly >/dev/null
```

Run a second one-Worker canary. Verify upload and final result download, then
verify the instance/EBS is gone. From inside that disposable Worker (or with
custom-policy simulation), also verify `GetObject`, `DeleteObject`, another
bucket, and a non-`jobs/*` key are denied.

The shared Worker profile cannot isolate one Job from another under `jobs/*`:
a malicious Job can overwrite another Job's key even though it cannot read or
delete it. True per-Job isolation requires per-Job role sessions/access points
or presigned uploads plus a Job-specific prefix; that is an architecture change,
not an IAM-policy-only change.

## Rollback

Rollback Manager and Worker independently from the administrator/control host.
Do not edit or detach policies on the shared `Manager` role.

```bash
# Manager rollback: use the current association ID returned by cutover.
ROLLBACK_JSON=$(aws ec2 replace-iam-instance-profile-association \
  --region "$AWS_REGION" \
  --association-id "$NEW_ASSOCIATION_ID" \
  --iam-instance-profile Name=Manager)
ROLLBACK_ASSOCIATION_ID=$(jq -r \
  '.IamInstanceProfileAssociation.AssociationId' <<<"$ROLLBACK_JSON")
ssh -i ~/.ssh/interview-key.pem -o BatchMode=yes ubuntu@172.31.46.129 \
  'sudo systemctl restart elastic-agent-manager.service'

# Worker rollback: restore only the previous managed policy.
aws iam attach-role-policy \
  --role-name "$EA_WORKER_ROLE" \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```

Keep the dedicated role/profile and narrow inline policies for at least one
full Job cycle and an observation window. Do not delete rollback resources in
the same change. The original `Manager` profile remains necessary for the other
six machines.
