#!/usr/bin/env bash
# Build an encrypted, credential-free Elastic-Agent worker AMI.
#
# The script can launch a new builder or finish an already-running one.  It
# deliberately requires a pinned base AMI and never resolves "latest" during a
# build, so the resulting manifest is reproducible and auditable.
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
VERIFY_SOURCE="$SCRIPT_DIR/golden_image_verify.py"

REGION="ap-northeast-1"
BASE_AMI=""
SUBNET_ID=""
SECURITY_GROUP_ID=""
KEY_NAME=""
SSH_KEY=""
SSH_USER="ubuntu"
RUNTIME_USER="ubuntu"
BUILDER_ID=""
INSTANCE_TYPE="t3.large"
ROOT_DISK_GB=20
ASSOCIATE_PUBLIC_IP=false
USE_PUBLIC_SSH=false
KEEP_BUILDER_ON_FAILURE=false
SOURCE_COMMIT=""
IMAGE_NAME=""
IMAGE_PROFILE="elastic-agent-worker-union-v1"
# Encrypted EBS snapshots can exceed the AWS CLI waiter's fixed ~10 minute
# window. Poll for up to one hour, while still failing immediately on a
# terminal image state and always letting the EXIT trap settle builder cost.
IMAGE_WAIT_ATTEMPTS=240
IMAGE_WAIT_SECONDS=15

CLAUDE_VERSION="2.1.181"
CODEX_VERSION="0.144.6"
CHROME_VERSION="150.0.7871.181-1"
CHROME_SHA256="fec50905f7b1235a440977a833476e0162874f5ca79e506cdf40b71af64d92f4"
CHROME_URL="https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_150.0.7871.181-1_amd64.deb"
UV_VERSION="0.11.31"
PLAYWRIGHT_VERSION="1.61.0"
PSUTIL_VERSION="7.2.2"

usage() {
  cat >&2 <<'EOF'
Build an encrypted, credential-free Elastic-Agent worker AMI.

Required for a new builder:
  --base-ami AMI --subnet SUBNET --security-group SG --key-name NAME --ssh-key PATH

Required for an existing builder:
  --builder-id INSTANCE --base-ami AMI --ssh-key PATH

Optional:
  --region REGION                 (default: ap-northeast-1)
  --ssh-user USER                 (default: ubuntu)
  --runtime-user USER             (default: ubuntu)
  --instance-type TYPE            (default: t3.large)
  --root-disk-gb N                (default: 20)
  --associate-public-ip           (new builders only; public egress address)
  --use-public-ssh                (default: use the private VPC address)
  --source-commit SHA             (default: current repository HEAD)
  --image-name NAME               (default: timestamped immutable name)
  --keep-builder-on-failure       (default: terminate to prevent cost leaks)

The final stdout line is JSON containing image_id, snapshot_ids, and tags.
Progress is written to stderr. No account/API/AWS credentials are copied.
EOF
  exit 2
}

while (($#)); do
  case "$1" in
    --region) REGION=${2:?}; shift 2 ;;
    --base-ami) BASE_AMI=${2:?}; shift 2 ;;
    --subnet) SUBNET_ID=${2:?}; shift 2 ;;
    --security-group) SECURITY_GROUP_ID=${2:?}; shift 2 ;;
    --key-name) KEY_NAME=${2:?}; shift 2 ;;
    --ssh-key) SSH_KEY=${2:?}; shift 2 ;;
    --ssh-user) SSH_USER=${2:?}; shift 2 ;;
    --runtime-user) RUNTIME_USER=${2:?}; shift 2 ;;
    --builder-id) BUILDER_ID=${2:?}; shift 2 ;;
    --instance-type) INSTANCE_TYPE=${2:?}; shift 2 ;;
    --root-disk-gb) ROOT_DISK_GB=${2:?}; shift 2 ;;
    --associate-public-ip) ASSOCIATE_PUBLIC_IP=true; shift ;;
    --use-public-ssh) USE_PUBLIC_SSH=true; shift ;;
    --source-commit) SOURCE_COMMIT=${2:?}; shift 2 ;;
    --image-name) IMAGE_NAME=${2:?}; shift 2 ;;
    --keep-builder-on-failure) KEEP_BUILDER_ON_FAILURE=true; shift ;;
    -h|--help) usage ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

log() { printf '[golden-ami] %s\n' "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

command -v aws >/dev/null || die "aws CLI is required"
command -v ssh >/dev/null || die "ssh is required"
command -v scp >/dev/null || die "scp is required"
test -r "$VERIFY_SOURCE" || die "missing verifier: $VERIFY_SOURCE"
test -n "$BASE_AMI" || die "--base-ami is required and must be pinned"
test -r "$SSH_KEY" || die "--ssh-key is required and must be readable"
[[ "$REGION" =~ ^[a-z]{2}-[a-z]+-[0-9]+$ ]] || die "invalid region"
[[ "$BASE_AMI" =~ ^ami-[0-9a-f]+$ ]] || die "invalid base AMI"
[[ "$ROOT_DISK_GB" =~ ^[0-9]+$ ]] && ((ROOT_DISK_GB >= 12 && ROOT_DISK_GB <= 2048)) \
  || die "root disk must be 12..2048 GiB"
[[ "$SSH_USER" =~ ^[a-z_][a-z0-9_-]*$ ]] || die "invalid SSH user"
[[ "$RUNTIME_USER" =~ ^[a-z_][a-z0-9_-]*$ ]] || die "invalid runtime user"

if [[ -z "$SOURCE_COMMIT" ]]; then
  SOURCE_COMMIT=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || true)
fi
[[ "$SOURCE_COMMIT" =~ ^[0-9a-f]{40}$ ]] || die "source commit must be a full 40-hex commit"
BUILD_TREE_SHA256=$(
  sha256sum \
    "$VERIFY_SOURCE" \
    "$SCRIPT_DIR/build_golden_ami.sh" \
    "$REPO_ROOT/src/elastic_agent/core/bootstrap_steps.py" \
  | sha256sum | awk '{print $1}'
)
BUILD_TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)
if [[ -z "$IMAGE_NAME" ]]; then
  IMAGE_NAME="elastic-agent-worker-union-v1-$(date -u +%Y%m%d%H%M%S)-${SOURCE_COMMIT:0:8}"
fi
[[ "$IMAGE_NAME" =~ ^[A-Za-z0-9._()/-]{3,128}$ ]] || die "invalid image name"

BASE_INFO=$(aws ec2 describe-images --region "$REGION" --image-ids "$BASE_AMI" \
  --query 'Images[0].[State,Architecture,VirtualizationType,RootDeviceType,RootDeviceName,EnaSupport,ImdsSupport,OwnerId,Name]' \
  --output text)
read -r BASE_STATE BASE_ARCH BASE_VIRT BASE_ROOT_TYPE ROOT_DEVICE BASE_ENA BASE_IMDS BASE_OWNER BASE_NAME <<<"$BASE_INFO"
[[ "$BASE_STATE" == "available" && "$BASE_ARCH" == "x86_64" \
   && "$BASE_VIRT" == "hvm" && "$BASE_ROOT_TYPE" == "ebs" \
   && "$BASE_ENA" == "True" && "$BASE_IMDS" == "v2.0" \
   && "$BASE_OWNER" == "099720109477" \
   && "$BASE_NAME" == ubuntu/images/hvm-ssd-gp3/ubuntu-*-amd64-server-* ]] \
  || die "base AMI is not a supported Canonical Ubuntu x86_64 image: $BASE_INFO"

IMAGE_ID=""
BUILD_SUCCEEDED=false
BUILDER_OWNED=false
cleanup_builder() {
  rc=$?
  if [[ "$BUILDER_OWNED" == true && \
        ("$BUILD_SUCCEEDED" == true || "$KEEP_BUILDER_ON_FAILURE" == false) ]]; then
    log "terminating builder $BUILDER_ID"
    aws ec2 terminate-instances --region "$REGION" --instance-ids "$BUILDER_ID" \
      >/dev/null 2>&1 || true
  elif [[ "$BUILDER_OWNED" == true ]]; then
    log "keeping failed builder $BUILDER_ID by request"
  fi
  exit "$rc"
}
trap cleanup_builder EXIT

if [[ -z "$BUILDER_ID" ]]; then
  test -n "$SUBNET_ID" || die "--subnet is required for a new builder"
  test -n "$SECURITY_GROUP_ID" || die "--security-group is required for a new builder"
  test -n "$KEY_NAME" || die "--key-name is required for a new builder"
  [[ "$SUBNET_ID" =~ ^subnet-[0-9a-f]+$ ]] || die "invalid subnet"
  [[ "$SECURITY_GROUP_ID" =~ ^sg-[0-9a-f]+$ ]] || die "invalid security group"

  NETWORK_JSON=$(python3 - "$SUBNET_ID" "$SECURITY_GROUP_ID" "$ASSOCIATE_PUBLIC_IP" <<'PY'
import json, sys
print(json.dumps([{
    "DeviceIndex": 0,
    "AssociatePublicIpAddress": sys.argv[3] == "true",
    "DeleteOnTermination": True,
    "SubnetId": sys.argv[1],
    "Groups": [sys.argv[2]],
}]))
PY
  )
  BLOCK_JSON=$(python3 - "$ROOT_DEVICE" "$ROOT_DISK_GB" <<'PY'
import json, sys
print(json.dumps([{
    "DeviceName": sys.argv[1],
    "Ebs": {
        "VolumeSize": int(sys.argv[2]),
        "VolumeType": "gp3",
        "Encrypted": True,
        "DeleteOnTermination": True,
    },
}]))
PY
  )
  log "launching encrypted builder from pinned $BASE_AMI"
  BUILDER_ID=$(aws ec2 run-instances \
    --region "$REGION" \
    --image-id "$BASE_AMI" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --network-interfaces "$NETWORK_JSON" \
    --block-device-mappings "$BLOCK_JSON" \
    --metadata-options 'HttpEndpoint=enabled,HttpTokens=required,HttpPutResponseHopLimit=1' \
    --tag-specifications \
      "ResourceType=instance,Tags=[{Key=Name,Value=elastic-agent-golden-builder},{Key=ManagedBy,Value=elastic-agent},{Key=Purpose,Value=golden-image-build}]" \
    --query 'Instances[0].InstanceId' --output text)
  BUILDER_OWNED=true
else
  [[ "$BUILDER_ID" =~ ^i-[0-9a-f]+$ ]] || die "invalid builder instance id"
fi

log "waiting for builder $BUILDER_ID"
aws ec2 wait instance-running --region "$REGION" --instance-ids "$BUILDER_ID"
BUILDER_INFO=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$BUILDER_ID" \
  --query 'Reservations[0].Instances[0].[ImageId,PrivateIpAddress,PublicIpAddress,State.Name,MetadataOptions.HttpTokens,MetadataOptions.HttpPutResponseHopLimit,Tags[?Key==`ManagedBy`]|[0].Value,Tags[?Key==`Purpose`]|[0].Value,Tags[?Key==`Role`]|[0].Value,Tags[?Key==`Name`]|[0].Value]' \
  --output text)
read -r ACTUAL_BASE PRIVATE_IP PUBLIC_IP BUILDER_STATE IMDS_TOKENS IMDS_HOP MANAGED_TAG PURPOSE_TAG ROLE_TAG NAME_TAG <<<"$BUILDER_INFO"
[[ "$ACTUAL_BASE" == "$BASE_AMI" && "$BUILDER_STATE" == "running" \
   && "$IMDS_TOKENS" == "required" && "$IMDS_HOP" == "1" \
   && "$MANAGED_TAG" == "elastic-agent" \
   && (("$PURPOSE_TAG" == "golden-image-build" \
        && "$NAME_TAG" == "elastic-agent-golden-builder") \
       || ("$ROLE_TAG" == "ami-builder" \
           && "$NAME_TAG" == elastic-agent-ami-builder*)) ]] \
  || die "builder source/state mismatch: $BUILDER_INFO"
BUILDER_OWNED=true
BUILDER_HOST="$PRIVATE_IP"
if [[ "$USE_PUBLIC_SSH" == true ]]; then
  [[ "$PUBLIC_IP" != "None" ]] \
    || die "--use-public-ssh requested but builder has no public IP"
  BUILDER_HOST="$PUBLIC_IP"
fi
[[ "$BUILDER_HOST" =~ ^[0-9a-fA-F:.]+$ ]] || die "builder has no usable address"

ROOT_VOLUME=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$BUILDER_ID" \
  --query "Reservations[0].Instances[0].BlockDeviceMappings[?DeviceName=='$ROOT_DEVICE'].Ebs.VolumeId | [0]" \
  --output text)
ROOT_ENCRYPTED=$(aws ec2 describe-volumes --region "$REGION" --volume-ids "$ROOT_VOLUME" \
  --query 'Volumes[0].Encrypted' --output text)
[[ "$ROOT_ENCRYPTED" == "True" ]] || die "builder root volume is not encrypted"

SSH_OPTS=(
  -i "$SSH_KEY"
  -o BatchMode=yes
  -o StrictHostKeyChecking=accept-new
  -o ConnectTimeout=8
  -o ServerAliveInterval=15
)
log "waiting for SSH on $BUILDER_HOST"
for _ in $(seq 1 120); do
  if ssh "${SSH_OPTS[@]}" "$SSH_USER@$BUILDER_HOST" true 2>/dev/null; then
    break
  fi
  sleep 5
done
ssh "${SSH_OPTS[@]}" "$SSH_USER@$BUILDER_HOST" true \
  || die "builder did not become SSH-ready"

log "copying standalone verifier"
scp "${SSH_OPTS[@]}" "$VERIFY_SOURCE" \
  "$SSH_USER@$BUILDER_HOST:/tmp/elastic-agent-image-verify" >/dev/null

log "installing pinned golden-image dependencies"
ssh "${SSH_OPTS[@]}" "$SSH_USER@$BUILDER_HOST" \
  sudo bash -s -- \
  "$BASE_AMI" "$SOURCE_COMMIT" "$BUILD_TREE_SHA256" "$BUILD_TIMESTAMP" \
  "$IMAGE_PROFILE" "$RUNTIME_USER" "$CLAUDE_VERSION" "$CODEX_VERSION" \
  "$CHROME_VERSION" "$CHROME_SHA256" "$CHROME_URL" \
  "$UV_VERSION" "$PLAYWRIGHT_VERSION" "$PSUTIL_VERSION" <<'REMOTE_BUILD'
set -Eeuo pipefail
BASE_AMI=$1
SOURCE_COMMIT=$2
BUILD_TREE_SHA256=$3
BUILD_TIMESTAMP=$4
IMAGE_PROFILE=$5
RUNTIME_USER=$6
CLAUDE_VERSION=$7
CODEX_VERSION=$8
shift 8
CHROME_VERSION=$1
CHROME_SHA256=$2
CHROME_URL=$3
UV_VERSION=$4
PLAYWRIGHT_VERSION=$5
PSUTIL_VERSION=$6

export DEBIAN_FRONTEND=noninteractive
cloud-init status --wait
apt-get -o DPkg::Lock::Timeout=600 update -qq
apt-get -o DPkg::Lock::Timeout=600 dist-upgrade -y -qq
apt-get -o DPkg::Lock::Timeout=600 install -y -qq \
  python3 python3-pip git curl rsync nodejs npm \
  xvfb xdotool wget ca-certificates awscli docker.io docker-buildx \
  python3-venv bubblewrap util-linux

chrome_deb=/tmp/google-chrome-stable.deb
curl --fail --location --silent --show-error "$CHROME_URL" -o "$chrome_deb"
printf '%s  %s\n' "$CHROME_SHA256" "$chrome_deb" | sha256sum --check --strict
apt-get -o DPkg::Lock::Timeout=600 install -y -qq "$chrome_deb"
test "$(dpkg-query -W -f='${Version}' google-chrome-stable)" = "$CHROME_VERSION"
rm -f "$chrome_deb"

# Long-running workers are immutable during a Job.  Ubuntu 24.04+ otherwise
# runs needrestart automatically from APT hooks, and apt-daily-upgrade can
# restart ea-runtime.service (taking opaque task children down with its cgroup).
install -d -m 0755 /etc/apt/apt.conf.d
cat > /etc/apt/apt.conf.d/99elastic-agent-no-background-upgrades <<'APTCONF'
APT::Periodic::Enable "0";
APT::Periodic::Update-Package-Lists "0";
APT::Periodic::Download-Upgradeable-Packages "0";
APT::Periodic::AutocleanInterval "0";
APT::Periodic::Unattended-Upgrade "0";
APTCONF
chmod 0644 /etc/apt/apt.conf.d/99elastic-agent-no-background-upgrades

install -d -m 0755 /etc/needrestart/conf.d
cat > /etc/needrestart/conf.d/99-elastic-agent.conf <<'NEEDRESTART'
$nrconf{restart} = 'l';
$nrconf{blacklist_rc} = [] unless ref($nrconf{blacklist_rc}) eq 'ARRAY';
push @{$nrconf{blacklist_rc}},
  qr/^(?:ea-runtime|elastic-agent-runtime|ea-task-supervisor|elastic-agent-task-supervisor|ea-task@.+|elastic-agent-task@.+)\.service$/;
NEEDRESTART
chmod 0644 /etc/needrestart/conf.d/99-elastic-agent.conf

background_update_units=(
  apt-daily.timer apt-daily-upgrade.timer
  apt-daily.service apt-daily-upgrade.service
  unattended-upgrades.service
)
systemctl disable --now apt-daily.timer apt-daily-upgrade.timer \
  >/dev/null 2>&1 || true
systemctl disable unattended-upgrades.service >/dev/null 2>&1 || true
systemctl mask --force "${background_update_units[@]}"
for unit in apt-daily.service apt-daily-upgrade.service; do
  while systemctl is-active --quiet "$unit"; do sleep 2; done
done
for unit in "${background_update_units[@]}"; do
  state=$(systemctl is-enabled "$unit" 2>/dev/null || true)
  test "$state" = masked
done

npm install -g "@anthropic-ai/claude-code@$CLAUDE_VERSION" \
  --include=optional --foreground-scripts --force
npm install -g "@openai/codex@$CODEX_VERSION" \
  --include=optional --foreground-scripts --force
pip3 install -q --break-system-packages \
  "pydantic==2.13.4" "pydantic-settings==2.14.1" \
  "websockets==16.0" "httpx==0.28.1" "pyyaml==6.0.3" \
  "psutil==$PSUTIL_VERSION" "playwright==$PLAYWRIGHT_VERSION" \
  "uv==$UV_VERSION"
install -o root -g root -m 0755 /tmp/elastic-agent-image-verify \
  /usr/local/bin/elastic-agent-image-verify
rm -f /tmp/elastic-agent-image-verify
usermod -aG docker "$RUNTIME_USER"

python3 - "$BASE_AMI" "$SOURCE_COMMIT" "$BUILD_TREE_SHA256" \
  "$BUILD_TIMESTAMP" "$IMAGE_PROFILE" "$CLAUDE_VERSION" "$CODEX_VERSION" \
  "$CHROME_VERSION" <<'PY'
import importlib.metadata as metadata
import json
import subprocess
import sys
from pathlib import Path

(base_ami, source_commit, tree_hash, built_at, profile, claude_version,
 codex_version, chrome_version) = sys.argv[1:]

def dpkg(name):
    return subprocess.check_output(
        ["dpkg-query", "-W", "-f=${Version}", name], text=True,
    ).strip()

system = [
    "python3", "python3-pip", "git", "curl", "rsync", "nodejs", "npm",
    "xvfb", "xdotool", "wget", "ca-certificates", "awscli",
    "python3-venv", "bubblewrap", "util-linux",
]
runtime = [
    "pydantic", "pydantic-settings", "websockets", "httpx", "pyyaml",
    "psutil", "playwright", "uv",
]
manifest = {
    "schema_version": 1,
    "image_profile": profile,
    "base_ami": base_ami,
    "source_commit": source_commit,
    "build_tree_sha256": tree_hash,
    "built_at": built_at,
    "components": {
        "system": {"packages": {name: dpkg(name) for name in system}},
        "agents": {"claude": claude_version, "codex": codex_version},
        "login": {
            "chrome_version": chrome_version,
            "system_packages": {name: dpkg(name) for name in ["xvfb", "xdotool"]},
            "python_packages": {
                name: metadata.version(name) for name in ["httpx", "websockets", "playwright"]
            },
        },
        "docker": {
            "system_packages": {
                name: dpkg(name) for name in ["docker.io", "docker-buildx"]
            },
        },
        "runtime": {"python_packages": {name: metadata.version(name) for name in runtime}},
    },
}
path = Path("/etc/elastic-agent/image-manifest.json")
path.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
path.chmod(0o644)
PY

# A union image carries Docker packages but standard-profile workers should not
# pay for an idle daemon. Docker bootstrap enables it only for Docker jobs.
systemctl disable --now docker.service docker.socket containerd.service \
  >/dev/null 2>&1 || true
REMOTE_BUILD

log "rebooting once so the baked kernel/users/groups are the state canaries boot"
ssh "${SSH_OPTS[@]}" "$SSH_USER@$BUILDER_HOST" sudo reboot || true
sleep 5
for _ in $(seq 1 120); do
  if ssh "${SSH_OPTS[@]}" "$SSH_USER@$BUILDER_HOST" true 2>/dev/null; then
    break
  fi
  sleep 5
done
ssh "${SSH_OPTS[@]}" "$SSH_USER@$BUILDER_HOST" true \
  || die "builder did not return after reboot"

log "validating every baked fast-path component"
ssh "${SSH_OPTS[@]}" "$SSH_USER@$BUILDER_HOST" sudo bash -s -- \
  "$CLAUDE_VERSION" "$CODEX_VERSION" "$SSH_USER" <<'REMOTE_VERIFY'
set -Eeuo pipefail
CLAUDE_VERSION=$1
CODEX_VERSION=$2
SSH_USER=$3
V=/usr/local/bin/elastic-agent-image-verify
$V system python3 python3-pip git curl rsync nodejs npm python3-venv bubblewrap util-linux
$V agent claude "$CLAUDE_VERSION"
$V agent codex "$CODEX_VERSION"
$V login httpx websockets playwright
$V docker
$V python pydantic pydantic-settings websockets httpx psutil
aws --version
uv --version

for unit in apt-daily.timer apt-daily-upgrade.timer \
  apt-daily.service apt-daily-upgrade.service unattended-upgrades.service; do
  state=$(systemctl is-enabled "$unit" 2>/dev/null || true)
  test "$state" = masked
done
grep -Fq 'APT::Periodic::Enable "0";' \
  /etc/apt/apt.conf.d/99elastic-agent-no-background-upgrades
grep -Fq "\$nrconf{restart} = 'l';" \
  /etc/needrestart/conf.d/99-elastic-agent.conf

chrome_tmp=$(mktemp -d /tmp/ea-chrome-check.XXXXXX)
chown "$SSH_USER:$SSH_USER" "$chrome_tmp"
runuser -u "$SSH_USER" -- timeout 30 google-chrome \
  --headless=new --no-sandbox --disable-gpu --disable-dev-shm-usage \
  --user-data-dir="$chrome_tmp" --dump-dom 'data:text/html,<title>ok</title>' \
  | grep -q '<title>ok</title>'
rm -rf -- "$chrome_tmp"

Xvfb :98 -screen 0 1280x1024x24 >/tmp/ea-golden-xvfb.log 2>&1 &
xvfb_pid=$!
sleep 1
kill -0 "$xvfb_pid"
kill "$xvfb_pid"
wait "$xvfb_pid" 2>/dev/null || true

systemctl enable --now docker
docker info >/dev/null
systemctl disable --now docker.service docker.socket containerd.service \
  >/dev/null 2>&1 || true
dpkg --audit
REMOTE_VERIFY

log "scrubbing machine identity, credentials, runtime state, and caches"
ssh "${SSH_OPTS[@]}" "$SSH_USER@$BUILDER_HOST" sudo bash -s -- \
  "$SSH_USER" "$RUNTIME_USER" <<'REMOTE_CLEAN'
set -Eeuo pipefail
SSH_USER=$1
RUNTIME_USER=$2
systemctl disable --now ea-runtime.service ea-task-supervisor.service \
  elastic-agent-runtime.service \
  docker.service docker.socket containerd.service >/dev/null 2>&1 || true
rm -f /etc/systemd/system/ea-runtime.service \
  /etc/systemd/system/ea-task-supervisor.service \
  /etc/systemd/system/elastic-agent-runtime.service \
  /usr/local/bin/ea-runtime.sh /usr/local/bin/ea-task-supervisor.sh
systemctl daemon-reload
rm -rf -- \
  "/home/$SSH_USER/.claude" "/home/$SSH_USER/.codex" \
  "/home/$SSH_USER/.config/google-chrome" "/home/$SSH_USER/.aws" \
  "/home/$SSH_USER/ea-tasks" "/home/$SSH_USER/ea-logs" \
  "/home/$RUNTIME_USER/.claude" "/home/$RUNTIME_USER/.codex" \
  "/home/$RUNTIME_USER/.config/google-chrome" "/home/$RUNTIME_USER/.aws" \
  "/home/$RUNTIME_USER/ea-tasks" "/home/$RUNTIME_USER/ea-logs" \
  /root/.claude /root/.codex /root/.config/google-chrome /root/.aws \
  /root/ea-tasks /root/ea-logs \
  /root/.cache/pip /root/.npm /var/lib/docker/* /var/lib/containerd/*
rm -f -- "/home/$SSH_USER/.bash_history" "/home/$RUNTIME_USER/.bash_history" \
  /root/.bash_history /root/.python_history
if find /home /root -xdev -type f \( -name auth.json -o -name credentials.json \
  -o -name '*.pem' \) -print -quit | grep -q .; then
  echo "credential-like file remains in builder" >&2
  exit 1
fi
apt-get clean
rm -rf -- /var/lib/apt/lists/* /tmp/* /var/tmp/*
find /var/log -type f -exec truncate -s 0 {} +
cloud-init clean --logs --machine-id --seed
rm -f /etc/ssh/ssh_host_* "/home/$SSH_USER/.ssh/authorized_keys" \
  "/home/$RUNTIME_USER/.ssh/authorized_keys" /root/.ssh/authorized_keys
sync
REMOTE_CLEAN

log "stopping clean builder before create-image"
aws ec2 stop-instances --region "$REGION" --instance-ids "$BUILDER_ID" >/dev/null
aws ec2 wait instance-stopped --region "$REGION" --instance-ids "$BUILDER_ID"

TAG_SPEC=$(python3 - \
  "$IMAGE_NAME" "$BASE_AMI" "$SOURCE_COMMIT" "$BUILD_TREE_SHA256" \
  "$BUILD_TIMESTAMP" "$IMAGE_PROFILE" "$CLAUDE_VERSION" "$CODEX_VERSION" <<'PY'
import json, sys
(name, base, commit, tree_hash, built_at, profile, claude, codex) = sys.argv[1:]
tags = [
    {"Key": "Name", "Value": name},
    {"Key": "ManagedBy", "Value": "elastic-agent"},
    {"Key": "Role", "Value": "worker-golden"},
    {"Key": "ImageProfile", "Value": profile},
    {"Key": "EnvironmentProfiles", "Value": "ubuntu-agent-v1,ubuntu-agent-docker-v1,ubuntu-agent-docker-sandbox-v1"},
    {"Key": "SourceAmi", "Value": base},
    {"Key": "SourceCommit", "Value": commit},
    {"Key": "BuildTreeSHA256", "Value": tree_hash},
    {"Key": "BuildTimestamp", "Value": built_at},
    {"Key": "ClaudeVersion", "Value": claude},
    {"Key": "CodexVersion", "Value": codex},
]
print(json.dumps([
    {"ResourceType": "image", "Tags": tags},
    {"ResourceType": "snapshot", "Tags": tags},
]))
PY
)
log "creating immutable image $IMAGE_NAME"
IMAGE_ID=$(aws ec2 create-image \
  --region "$REGION" \
  --instance-id "$BUILDER_ID" \
  --name "$IMAGE_NAME" \
  --description "Elastic-Agent union worker golden image; credential-free; $BUILD_TIMESTAMP" \
  --no-reboot \
  --tag-specifications "$TAG_SPEC" \
  --query ImageId --output text)
IMAGE_STATE=""
for attempt in $(seq 1 "$IMAGE_WAIT_ATTEMPTS"); do
  if ! IMAGE_STATE=$(aws ec2 describe-images \
    --region "$REGION" --image-ids "$IMAGE_ID" \
    --query 'Images[0].State' --output text 2>/dev/null); then
    IMAGE_STATE="not-visible"
  fi
  if [[ "$IMAGE_STATE" == "available" ]]; then
    break
  fi
  case "$IMAGE_STATE" in
    failed|error|invalid|deregistered)
      IMAGE_REASON=$(aws ec2 describe-images \
        --region "$REGION" --image-ids "$IMAGE_ID" \
        --query 'Images[0].StateReason.[Code,Message]' --output text \
        2>/dev/null || true)
      die "created AMI entered terminal state $IMAGE_STATE: $IMAGE_ID (${IMAGE_REASON:-no reason reported})"
      ;;
    pending|transient|not-visible|None|"") ;;
    *) die "created AMI returned unexpected state $IMAGE_STATE: $IMAGE_ID" ;;
  esac
  if ((attempt == 1 || attempt % 4 == 0)); then
    log "waiting for image $IMAGE_ID (state=$IMAGE_STATE, attempt=$attempt/$IMAGE_WAIT_ATTEMPTS)"
  fi
  ((attempt == IMAGE_WAIT_ATTEMPTS)) || sleep "$IMAGE_WAIT_SECONDS"
done
[[ "$IMAGE_STATE" == "available" ]] \
  || die "created AMI did not become available within $((IMAGE_WAIT_ATTEMPTS * IMAGE_WAIT_SECONDS)) seconds: $IMAGE_ID (last state=$IMAGE_STATE)"

IMAGE_CHECK=$(aws ec2 describe-images --region "$REGION" --image-ids "$IMAGE_ID" \
  --query 'Images[0].[State,Architecture,VirtualizationType,RootDeviceType,EnaSupport,ImdsSupport,Tags[?Key==`ManagedBy`]|[0].Value,Tags[?Key==`Role`]|[0].Value]' \
  --output text)
[[ "$IMAGE_CHECK" == $'available\tx86_64\thvm\tebs\tTrue\tv2.0\telastic-agent\tworker-golden' ]] \
  || die "created AMI failed invariant check: $IMAGE_CHECK"
mapfile -t SNAPSHOT_IDS < <(aws ec2 describe-images --region "$REGION" --image-ids "$IMAGE_ID" \
  --query 'Images[0].BlockDeviceMappings[?Ebs].Ebs.SnapshotId' --output text | tr '\t' '\n')
((${#SNAPSHOT_IDS[@]} > 0)) || die "created AMI has no EBS snapshot"
for snapshot in "${SNAPSHOT_IDS[@]}"; do
  encrypted=$(aws ec2 describe-snapshots --region "$REGION" --snapshot-ids "$snapshot" \
    --query 'Snapshots[0].Encrypted' --output text)
  [[ "$encrypted" == "True" ]] || die "snapshot $snapshot is not encrypted"
done

BUILD_SUCCEEDED=true
python3 - "$IMAGE_ID" "$REGION" "$IMAGE_NAME" "$BASE_AMI" "$SOURCE_COMMIT" \
  "$BUILD_TREE_SHA256" "$BUILD_TIMESTAMP" "${SNAPSHOT_IDS[*]}" <<'PY'
import json, sys
image_id, region, name, base, commit, tree_hash, built_at, snapshots = sys.argv[1:]
print(json.dumps({
    "image_id": image_id,
    "region": region,
    "name": name,
    "snapshot_ids": snapshots.split(),
    "tags": {
        "SourceAmi": base,
        "SourceCommit": commit,
        "BuildTreeSHA256": tree_hash,
        "BuildTimestamp": built_at,
    },
}, sort_keys=True))
PY
