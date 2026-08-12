# Elastic-Agent IAM least-privilege cutover

This runbook is intentionally scoped to account `297645381734`, Region
`ap-northeast-1`, Manager instance `i-07988886030e168cc`, result bucket
`elastic-agent-results-297645381734`, and Worker role/profile
`elastic-agent-worker`.

**Status (2026-07-22): completed.** The target now uses association
`iip-assoc-01f75381926371ea2` and profile `elastic-agent-manager`; the shared
`Manager` profile remains on six other instances. Sections 1–5 preserve the
original one-time cutover procedure and its pre-cutover guard values; do not
blindly rerun its create/replace commands against the completed deployment.
Policy updates should repeat the validation and canaries, then update the
existing inline policies in place. The rollback section is deliberately
self-contained for a fresh administrator shell.

Run every mutating command from a separate administrator/control instance, not
from `i-07988886030e168cc`. The new Manager role deliberately cannot replace
its own instance profile or edit IAM.

## Why the existing Manager role must not be tightened

Before cutover, the instance profile `Manager` was attached to seven instances:

```text
i-0e0eb4d47c6e3a075  i-03e9984e1c983a1a0  i-06c940fa448e9059c
i-0351fab2f447ebfaf  i-0b1a45f7f632c07f0  i-0f51cc51d16bbda74
i-07988886030e168cc
```

The target on the last line has since moved to the dedicated profile, leaving
the first six on `Manager`.

Its role has `AdministratorAccess`, `AmazonEC2FullAccess`, and
`AmazonS3FullAccess`, plus unrelated inline policies. Detaching any of those
from the shared role can break CCM and other machines. Create a dedicated
`elastic-agent-manager` role/profile and replace only the association on
`i-07988886030e168cc`.

The production audit found 65 persisted JobSpecs, zero `setup.s3_datasets`
entries, and zero `run.secret_env` entries. The Worker policy writes only under
`jobs/*` and reads only `jobs/datasets/*`; the latter is the reviewed staging
namespace for `setup.s3_datasets`. Do not restore `AmazonS3FullAccess` or grant
object reads outside that dataset prefix.

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
`deploy/aws_manager.py` environment must match these IDs before the first Job
launch.
The IAM policy pins `ec2:InstanceType` to the same common x86_64 T3,
M5/M6i/M7i, C5/C6i/C7i, and R5/R6i/R7i set as the production Job allowlist
(`large` through `4xlarge`, with T3 ending at `2xlarge`). Keep the two sets
identical. Before enabling another per-Job instance type, add it to both
allowlists, re-run simulation for that type, and deploy the two changes
together; otherwise `RunInstances` must remain denied. The example below
exercises `r5.2xlarge`.

```bash
export AWS_REGION=ap-northeast-1
export EA_MANAGER_INSTANCE=i-07988886030e168cc
export EA_MANAGER_ROLE=elastic-agent-manager
export EA_MANAGER_PROFILE=elastic-agent-manager
export EA_WORKER_ROLE=elastic-agent-worker
export EA_RESULTS_BUCKET=elastic-agent-results-297645381734
export FINAL_AMI_ID=ami-0aec7ffcbe44c6f7a
export FINAL_WORKER_SECURITY_GROUP_ID=sg-05c68220f901fb555

[[ "$FINAL_AMI_ID" =~ ^ami-[0-9a-f]{17}$ ]]
[[ "$FINAL_WORKER_SECURITY_GROUP_ID" =~ ^sg-[0-9a-f]{17}$ ]]

EA_IAM_TMP=$(mktemp -d)
cp deploy/aws/elastic-agent-manager-policy.json "$EA_IAM_TMP/manager-policy.json"
cp deploy/aws/elastic-agent-worker-policy.json "$EA_IAM_TMP/worker-policy.json"
cp deploy/aws/elastic-agent-results-bucket-policy.json \
  "$EA_IAM_TMP/results-bucket-policy.json"
jq -e . "$EA_IAM_TMP/manager-policy.json" "$EA_IAM_TMP/worker-policy.json" \
  "$EA_IAM_TMP/results-bucket-policy.json"

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
test "$(aws ec2 describe-addresses --region "$AWS_REGION" \
  --filters Name=tag:ManagedBy,Values=elastic-agent \
  --query 'length(Addresses[?AssociationId!=null])' --output text)" = 0

ssh -i ~/.ssh/interview-key.pem -o BatchMode=yes \
  ubuntu@172.31.46.129 \
  'systemctl is-active --quiet elastic-agent-manager.service'
curl --fail --silent --show-error \
  https://elastic-agent.claude-code-manager.com/api/health >/dev/null

```

Do not proceed if a Job, account lease, EIP attachment, Worker bootstrap, or
collection is active even if the EC2 query happens to be empty.
The final launcher requires the dedicated role, so stage its immutable release
now but activate it only after step 4 has replaced and verified the instance
profile.

## 2. Validate and simulate before any IAM write

```bash
aws accessanalyzer validate-policy --policy-type IDENTITY_POLICY \
  --policy-document "file://$EA_IAM_TMP/manager-policy.json" \
  | jq -e '.findings | length == 0'
aws accessanalyzer validate-policy --policy-type IDENTITY_POLICY \
  --policy-document "file://$EA_IAM_TMP/worker-policy.json" \
  | jq -e '.findings | length == 0'
aws accessanalyzer validate-policy --policy-type RESOURCE_POLICY \
  --validate-policy-resource-type AWS::S3::Bucket \
  --policy-document "file://$EA_IAM_TMP/results-bucket-policy.json" \
  | jq -e '.findings | length == 0'

WORKER_POLICY=$(jq -c . "$EA_IAM_TMP/worker-policy.json")
MANAGER_POLICY=$(jq -c . "$EA_IAM_TMP/manager-policy.json")
# IAM's PolicyInputList member limit is 131,072 characters. Simulate the full
# policy so future explicit Deny/cross-statement constraints remain effective.
test "$(printf %s "$MANAGER_POLICY" | wc -c)" -le 131072

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

# EIP-bound RunInstances may tag its primary ENI, and detach requires both the
# tagged EIP and tagged ENI. A foreign tag must deny both resources.
test "$(aws iam simulate-custom-policy --policy-input-list "$MANAGER_POLICY" \
  --action-names ec2:CreateTags \
  --resource-arns \
    arn:aws:ec2:ap-northeast-1:297645381734:network-interface/eni-0123456789abcdef0 \
  --context-entries \
    'ContextKeyName=aws:RequestedRegion,ContextKeyValues=ap-northeast-1,ContextKeyType=string' \
    'ContextKeyName=aws:RequestTag/ManagedBy,ContextKeyValues=elastic-agent,ContextKeyType=string' \
    'ContextKeyName=aws:TagKeys,ContextKeyValues=ManagedBy,ElasticAgentJob,ElasticAgentController,ElasticAgentLease,ElasticAgentAccount,AccountId,Role,ContextKeyType=stringList' \
    'ContextKeyName=ec2:CreateAction,ContextKeyValues=RunInstances,ContextKeyType=string' \
  --query 'EvaluationResults[0].EvalDecision' --output text)" = allowed
test "$(aws iam simulate-custom-policy --policy-input-list "$MANAGER_POLICY" \
  --action-names ec2:DisassociateAddress \
  --resource-arns \
    arn:aws:ec2:ap-northeast-1:297645381734:elastic-ip/eipalloc-0123456789abcdef0 \
    arn:aws:ec2:ap-northeast-1:297645381734:network-interface/eni-0123456789abcdef0 \
  --context-entries \
    'ContextKeyName=aws:RequestedRegion,ContextKeyValues=ap-northeast-1,ContextKeyType=string' \
    'ContextKeyName=ec2:ResourceTag/ManagedBy,ContextKeyValues=elastic-agent,ContextKeyType=string' \
  --query 'length(EvaluationResults[?EvalDecision!=`allowed`])' \
  --output text)" = 0
test "$(aws iam simulate-custom-policy --policy-input-list "$MANAGER_POLICY" \
  --action-names ec2:DisassociateAddress \
  --resource-arns \
    arn:aws:ec2:ap-northeast-1:297645381734:elastic-ip/eipalloc-0123456789abcdef0 \
    arn:aws:ec2:ap-northeast-1:297645381734:network-interface/eni-0123456789abcdef0 \
  --context-entries \
    'ContextKeyName=aws:RequestedRegion,ContextKeyValues=ap-northeast-1,ContextKeyType=string' \
    'ContextKeyName=ec2:ResourceTag/ManagedBy,ContextKeyValues=foreign,ContextKeyType=string' \
  --query 'length(EvaluationResults[?EvalDecision!=`implicitDeny`])' \
  --output text)" = 0

# A representative valid launch must be allowed across every resource type.
test "$(aws iam simulate-custom-policy --policy-input-list "$MANAGER_POLICY" \
  --action-names ec2:RunInstances \
  --resource-arns \
    "arn:aws:ec2:ap-northeast-1::image/$FINAL_AMI_ID" \
    arn:aws:ec2:ap-northeast-1:297645381734:subnet/subnet-0c1db80817d054277 \
    arn:aws:ec2:ap-northeast-1:297645381734:security-group/sg-05c68220f901fb555 \
    arn:aws:ec2:ap-northeast-1:297645381734:key-pair/panyuexi \
    arn:aws:ec2:ap-northeast-1:297645381734:network-interface/eni-0123456789abcdef0 \
    arn:aws:ec2:ap-northeast-1:297645381734:volume/vol-0123456789abcdef0 \
    arn:aws:ec2:ap-northeast-1:297645381734:instance/i-0123456789abcdef0 \
  --context-entries \
    'ContextKeyName=aws:RequestedRegion,ContextKeyValues=ap-northeast-1,ContextKeyType=string' \
    'ContextKeyName=aws:RequestTag/ManagedBy,ContextKeyValues=elastic-agent,ContextKeyType=string' \
    'ContextKeyName=aws:TagKeys,ContextKeyValues=ManagedBy,Name,ElasticAgentJob,ElasticAgentShardIndex,ElasticAgentController,ContextKeyType=stringList' \
    'ContextKeyName=ec2:VolumeType,ContextKeyValues=gp3,ContextKeyType=string' \
    'ContextKeyName=ec2:Encrypted,ContextKeyValues=true,ContextKeyType=boolean' \
    'ContextKeyName=ec2:VolumeSize,ContextKeyValues=100,ContextKeyType=numeric' \
    'ContextKeyName=ec2:InstanceType,ContextKeyValues=r5.2xlarge,ContextKeyType=string' \
    'ContextKeyName=ec2:MetadataHttpTokens,ContextKeyValues=required,ContextKeyType=string' \
    'ContextKeyName=ec2:MetadataHttpPutResponseHopLimit,ContextKeyValues=1,ContextKeyType=numeric' \
    'ContextKeyName=ec2:InstanceProfile,ContextKeyValues=arn:aws:iam::297645381734:instance-profile/elastic-agent-worker,ContextKeyType=string' \
  --query 'length(EvaluationResults[?EvalDecision!=`allowed`])' \
  --output text)" = 0

# Checkpoint retention may delete only the Manager's internal immutable
# generations. Public result objects remain non-deletable by this role.
test "$(aws iam simulate-custom-policy --policy-input-list "$MANAGER_POLICY" \
  --action-names s3:DeleteObject \
  --resource-arns \
    arn:aws:s3:::elastic-agent-results-297645381734/jobs/.elastic-agent-checkpoints/job-test/checkpoint-blobs/deadbeef \
  --query 'EvaluationResults[0].EvalDecision' --output text)" = allowed
test "$(aws iam simulate-custom-policy --policy-input-list "$MANAGER_POLICY" \
  --action-names s3:DeleteObject \
  --resource-arns \
    arn:aws:s3:::elastic-agent-results-297645381734/jobs/job-test/result.json \
  --query 'EvaluationResults[0].EvalDecision' --output text)" = implicitDeny
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
    "env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY \
      -u AWS_SESSION_TOKEN -u AWS_PROFILE -u AWS_DEFAULT_PROFILE \
      AWS_SHARED_CREDENTIALS_FILE=/dev/null AWS_CONFIG_FILE=/dev/null \
      /home/ubuntu/elastic-agent/.venv/bin/python -c \
      'import boto3; print(boto3.client(\"sts\", region_name=\"ap-northeast-1\").get_caller_identity()[\"Arn\"])'")
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

Now install the immutable release and versioned unit. The state directory must
pre-exist because systemd constructs `ReadWritePaths` before the launcher can
create it. Back up the old release, unit, and both environment files; install
the `/etc` files with explicit ownership/modes. `systemctl restart` does not
return until the unit's local health `ExecStartPost` succeeds. A healthy process
also proves that the launcher's exact STS role check passed with alternate AWS
credential providers disabled.

```bash
export RELEASE=/home/ubuntu/elastic-agent.release-<git-short-sha>
export BACKUP_SUFFIX=pre-<git-short-sha>

# Extract/clone directly into the final path before installing the editable
# project. Moving it afterwards leaves the .pth entry pointing at the old path.
# The AWS extra is mandatory: boto3 is deliberately not a base dependency, and
# aws_manager.py will fail closed during AMI/STS validation when it is absent.
ssh -i ~/.ssh/interview-key.pem -o BatchMode=yes ubuntu@172.31.46.129 \
  bash -s -- "$RELEASE" <<'REMOTE'
set -Eeuo pipefail
release=$1
cd "$release"
/home/ubuntu/.local/bin/uv sync --frozen --no-dev --extra aws
.venv/bin/python -c 'import boto3'
REMOTE

ssh -i ~/.ssh/interview-key.pem -o BatchMode=yes ubuntu@172.31.46.129 \
  sudo bash -s -- "$RELEASE" "$BACKUP_SUFFIX" \
  "$FINAL_AMI_ID" "$FINAL_WORKER_SECURITY_GROUP_ID" <<'REMOTE'
set -Eeuo pipefail
release=$1
suffix=$2
final_ami=$3
final_sg=$4
state_dir=/home/ubuntu/.elastic-agent-demo
test -x "$release/.venv/bin/python"
test -f "$release/deploy/aws_manager.py"
test -f "$release/deploy/aws/elastic-agent-manager.service"
test -f "$release/deploy/aws/elastic-agent-manager.aws.env"

old_release=$(readlink -f /home/ubuntu/elastic-agent)
ln -sfn "$old_release" "/home/ubuntu/elastic-agent.rollback-$suffix"
cp -a /etc/systemd/system/elastic-agent-manager.service \
  "/etc/systemd/system/elastic-agent-manager.service.$suffix"
cp -a /etc/elastic-agent-manager.env "/etc/elastic-agent-manager.env.$suffix"
if test -e /etc/elastic-agent-manager.aws.env; then
  cp -a /etc/elastic-agent-manager.aws.env \
    "/etc/elastic-agent-manager.aws.env.$suffix"
else
  : >"/etc/elastic-agent-manager.aws.env.$suffix.absent"
fi

install -d -o ubuntu -g ubuntu -m 0700 "$state_dir"
install -o root -g root -m 0600 \
  "$release/deploy/aws/elastic-agent-manager.aws.env" \
  /etc/elastic-agent-manager.aws.env
install -o root -g root -m 0644 \
  "$release/deploy/aws/elastic-agent-manager.service" \
  /etc/systemd/system/elastic-agent-manager.service
ln -sfn "$release" /home/ubuntu/elastic-agent.next
mv -Tf /home/ubuntu/elastic-agent.next /home/ubuntu/elastic-agent

set -a
source /etc/elastic-agent-manager.env
source /etc/elastic-agent-manager.aws.env
set +a
test "$ELASTIC_AGENT_STATE_DIR" = "$state_dir"
test "$ELASTIC_AGENT_AWS_AMI_ID" = "$final_ami"
test "$ELASTIC_AGENT_AWS_WORKER_SECURITY_GROUP_IDS" = "$final_sg"
test "$ELASTIC_AGENT_AWS_EXPECTED_ROLE_NAME" = elastic-agent-manager
test "$(stat -c '%U:%G:%a' /etc/elastic-agent-manager.env)" = root:root:600
test "$(stat -c '%U:%G:%a' /etc/elastic-agent-manager.aws.env)" = root:root:600
systemctl daemon-reload
systemd-analyze verify /etc/systemd/system/elastic-agent-manager.service
systemctl restart elastic-agent-manager.service
curl -fsS http://127.0.0.1:8080/api/health >/dev/null
REMOTE

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

# Explicitly deny plaintext S3 transport; this does not grant new access.
aws s3api put-bucket-policy \
  --bucket "$EA_RESULTS_BUCKET" \
  --policy "file://$EA_IAM_TMP/results-bucket-policy.json"
aws s3api get-bucket-policy --bucket "$EA_RESULTS_BUCKET" \
  --query Policy --output text | jq -e \
  '.Statement[] | select(.Sid == "DenyInsecureTransport")'
```

Run a second one-Worker canary. Verify upload and final result download, then
verify the instance/EBS is gone. From inside that disposable Worker (or with
custom-policy simulation), verify `GetObject` succeeds only for
`jobs/datasets/*`, while result-object reads, `DeleteObject`, another bucket,
and a non-`jobs/*` key are denied.

The shared Worker profile cannot isolate one Job from another under `jobs/*`:
a malicious Job can overwrite another Job's key even though it cannot read or
delete it. True per-Job isolation requires per-Job role sessions/access points
or presigned uploads plus a Job-specific prefix; that is an architecture change,
not an IAM-policy-only change.

## Rollback

Prefer an application-release rollback that keeps the dedicated Manager role,
private SGs, narrow Worker policy, and bucket TLS policy. Run from a fresh
administrator shell: explicitly choose a backup suffix, discover the current
association, and verify every target before changing state. A safe backup must
contain the hardened unit and AWS env; the original `pre-7627c81` backup does
not and is intentionally rejected by these guards.

```bash
export AWS_REGION=ap-northeast-1
export EA_MANAGER_INSTANCE=i-07988886030e168cc
export EA_WORKER_ROLE=elastic-agent-worker
export BACKUP_SUFFIX=pre-<rollback-target-sha>

CURRENT_ASSOCIATION_ID=$(aws ec2 \
  describe-iam-instance-profile-associations \
  --region "$AWS_REGION" \
  --query "IamInstanceProfileAssociations[?InstanceId=='$EA_MANAGER_INSTANCE'].AssociationId | [0]" \
  --output text)
CURRENT_PROFILE_ARN=$(aws ec2 \
  describe-iam-instance-profile-associations \
  --region "$AWS_REGION" --association-ids "$CURRENT_ASSOCIATION_ID" \
  --query 'IamInstanceProfileAssociations[0].IamInstanceProfile.Arn' \
  --output text)
test "$CURRENT_PROFILE_ARN" = \
  arn:aws:iam::297645381734:instance-profile/elastic-agent-manager

# Refuse a legacy backup that would silently drop the hardened launcher/env.
ssh -i ~/.ssh/interview-key.pem -o BatchMode=yes ubuntu@172.31.46.129 \
  sudo bash -s -- "$BACKUP_SUFFIX" <<'REMOTE'
set -Eeuo pipefail
suffix=$1
test -f "/etc/systemd/system/elastic-agent-manager.service.$suffix"
test -f "/etc/elastic-agent-manager.env.$suffix"
test -f "/etc/elastic-agent-manager.aws.env.$suffix"
test ! -e "/etc/elastic-agent-manager.aws.env.$suffix.absent"
rollback_release=$(readlink -f "/home/ubuntu/elastic-agent.rollback-$suffix")
test -x "$rollback_release/.venv/bin/python"
test -f "$rollback_release/deploy/aws_manager.py"
grep -q '^ELASTIC_AGENT_AWS_EXPECTED_ROLE_NAME=elastic-agent-manager$' \
  "/etc/elastic-agent-manager.aws.env.$suffix"
REMOTE

# Restore application/unit/env while retaining every hardened AWS boundary.
ssh -i ~/.ssh/interview-key.pem -o BatchMode=yes ubuntu@172.31.46.129 \
  sudo bash -s -- "$BACKUP_SUFFIX" <<'REMOTE'
set -Eeuo pipefail
suffix=$1
cp -a "/etc/systemd/system/elastic-agent-manager.service.$suffix" \
  /etc/systemd/system/elastic-agent-manager.service
cp -a "/etc/elastic-agent-manager.env.$suffix" /etc/elastic-agent-manager.env
cp -a "/etc/elastic-agent-manager.aws.env.$suffix" \
  /etc/elastic-agent-manager.aws.env
rollback_release=$(readlink -f "/home/ubuntu/elastic-agent.rollback-$suffix")
ln -sfn "$rollback_release" /home/ubuntu/elastic-agent.next
mv -Tf /home/ubuntu/elastic-agent.next /home/ubuntu/elastic-agent
systemctl daemon-reload
systemctl restart elastic-agent-manager.service
curl -fsS http://127.0.0.1:8080/api/health >/dev/null
REMOTE
curl --fail --silent --show-error \
  https://elastic-agent.claude-code-manager.com/api/health >/dev/null
```

If the dedicated role itself is the proven cause of an outage, replacing it
with shared profile `Manager` is a **break-glass security downgrade**: that role
has administrator/EC2/S3-wide permissions and is shared by six other machines.
Discover the association again in the same fresh shell; never reuse a stale
`NEW_ASSOCIATION_ID`. Similarly, restoring `AmazonS3FullAccess` is only for a
proven Worker-policy failure. These actions do not restore application
compatibility by themselves and must not be part of a routine release rollback.

```bash
CURRENT_ASSOCIATION_ID=$(aws ec2 \
  describe-iam-instance-profile-associations \
  --region "$AWS_REGION" \
  --query "IamInstanceProfileAssociations[?InstanceId=='$EA_MANAGER_INSTANCE'].AssociationId | [0]" \
  --output text)
test "$(aws ec2 describe-iam-instance-profile-associations \
  --region "$AWS_REGION" --association-ids "$CURRENT_ASSOCIATION_ID" \
  --query 'IamInstanceProfileAssociations[0].IamInstanceProfile.Arn' \
  --output text)" = \
  arn:aws:iam::297645381734:instance-profile/elastic-agent-manager

ROLLBACK_JSON=$(aws ec2 replace-iam-instance-profile-association \
  --region "$AWS_REGION" \
  --association-id "$CURRENT_ASSOCIATION_ID" \
  --iam-instance-profile Name=Manager)
ROLLBACK_ASSOCIATION_ID=$(jq -r \
  '.IamInstanceProfileAssociation.AssociationId' <<<"$ROLLBACK_JSON")
test -n "$ROLLBACK_ASSOCIATION_ID"
ssh -i ~/.ssh/interview-key.pem -o BatchMode=yes ubuntu@172.31.46.129 \
  'sudo systemctl restart elastic-agent-manager.service && \
   curl -fsS http://127.0.0.1:8080/api/health >/dev/null'

# Separate Worker break glass, only if its narrow policy is the proven cause.
aws iam attach-role-policy \
  --role-name "$EA_WORKER_ROLE" \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```

Do not restore the old shared SG `sg-056408de7cf971e02`: it exposes SSH and
multiple ports to `0.0.0.0/0`. Do not assume a healthy Manager means Worker
launches work. If a rollback selects another AMI, update the AMI resource pin in
the Manager IAM policy and `ELASTIC_AGENT_AWS_AMI_ID` together, run Access
Analyzer/full-policy simulation, and complete a create/upload/terminate canary.
The Canonical break-glass image also requires the launcher flag and an explicit
IAM image pin; the flag alone is insufficient. After any role/S3 break glass,
restore the dedicated/narrow policies immediately and repeat both ordinary and
EIP canaries.

Keep the dedicated role/profile, narrow inline policies, and recoverable release
backups through an observation window. Do not delete rollback resources in the
same change. The original `Manager` profile remains necessary for the other six
machines.
