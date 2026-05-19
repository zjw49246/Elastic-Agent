# Aliyun (Alibaba Cloud) Prerequisites Setup

> This guide covers the one-time manual setup of VPC, VSwitch, Security Group, and Key Pair
> required before running Elastic-Agent with the Aliyun ECS provider.

## Prerequisites

- An Alibaba Cloud account with ECS permissions
- RAM sub-account with `AliyunECSFullAccess` policy (recommended over root account)
- Alibaba Cloud CLI (`aliyun`) installed, or use the web console

## 1. VPC (Virtual Private Cloud)

You can reuse an existing VPC. If you need a new one:

**Console:** VPC Console > VPC > Create VPC

| Parameter | Recommended Value |
|---|---|
| Region | Same region as your Manager (e.g. `cn-hangzhou`) |
| CIDR Block | `172.16.0.0/12` |
| Name | `elastic-agent-vpc` |

**CLI:**
```bash
aliyun vpc CreateVpc \
  --RegionId cn-hangzhou \
  --CidrBlock "172.16.0.0/12" \
  --VpcName "elastic-agent-vpc"
```

Note the **VPC ID** (e.g. `vpc-bp1xxxxx`).

## 2. VSwitch (Subnet)

Manager and Workers must be in the **same VSwitch** for internal communication.

**Console:** VPC Console > VSwitch > Create VSwitch

| Parameter | Recommended Value |
|---|---|
| VPC | Select the VPC from step 1 |
| Zone | Any available zone (e.g. `cn-hangzhou-h`) |
| CIDR Block | `172.16.0.0/24` |
| Name | `elastic-agent-vswitch` |

**CLI:**
```bash
aliyun vpc CreateVSwitch \
  --VpcId vpc-bp1xxxxx \
  --ZoneId cn-hangzhou-h \
  --CidrBlock "172.16.0.0/24" \
  --VSwitchName "elastic-agent-vswitch"
```

Note the **VSwitch ID** (e.g. `vsw-bp1xxxxx`) and put it in `config.yaml`:
```yaml
provider:
  aliyun:
    vswitch_id: "vsw-bp1xxxxx"
```

## 3. Security Group

**Console:** ECS Console > Security Groups > Create Security Group

| Parameter | Value |
|---|---|
| VPC | Select the VPC from step 1 |
| Name | `elastic-agent-sg` |
| Type | Normal (not Enterprise) |

**Inbound Rules:**

| Priority | Protocol | Port Range | Source | Description |
|---|---|---|---|---|
| 1 | TCP | 22 | VPC CIDR (`172.16.0.0/12`) | SSH for Bootstrap |
| 1 | TCP | 8080 | VPC CIDR (`172.16.0.0/12`) | Worker Runtime |

> Workers initiate outbound WebSocket connections to Manager, so no inbound rules
> are needed from outside the VPC.

**CLI:**
```bash
# Create security group
aliyun ecs CreateSecurityGroup \
  --RegionId cn-hangzhou \
  --VpcId vpc-bp1xxxxx \
  --SecurityGroupName "elastic-agent-sg"

# Add SSH rule (VPC internal)
aliyun ecs AuthorizeSecurityGroup \
  --SecurityGroupId sg-bp1xxxxx \
  --IpProtocol tcp \
  --PortRange 22/22 \
  --SourceCidrIp "172.16.0.0/12"

# Add Runtime port rule (VPC internal)
aliyun ecs AuthorizeSecurityGroup \
  --SecurityGroupId sg-bp1xxxxx \
  --IpProtocol tcp \
  --PortRange 8080/8080 \
  --SourceCidrIp "172.16.0.0/12"
```

Note the **Security Group ID** (e.g. `sg-bp1xxxxx`):
```yaml
provider:
  aliyun:
    security_group_id: "sg-bp1xxxxx"
```

## 4. Key Pair

**Console:** ECS Console > Key Pairs > Create Key Pair

| Parameter | Value |
|---|---|
| Name | `elastic-agent-key` |
| Type | Auto-create |

Download the private key file (`.pem`) and save it:

```bash
mv elastic-agent-key.pem ~/.ssh/elastic-agent-aliyun.pem
chmod 600 ~/.ssh/elastic-agent-aliyun.pem
```

**CLI:**
```bash
aliyun ecs CreateKeyPair \
  --RegionId cn-hangzhou \
  --KeyPairName "elastic-agent-key"
# Save the PrivateKeyBody from the response to a .pem file
```

Config:
```yaml
provider:
  aliyun:
    key_pair_name: "elastic-agent-key"
    ssh_key_path: "~/.ssh/elastic-agent-aliyun.pem"
```

## 5. Custom Image (Optional)

Pre-installing Python 3.11, Node.js 20, and common tools into a custom image
speeds up Bootstrap by ~3 minutes per Worker.

1. Launch a base Ubuntu 22.04 ECS instance
2. Install dependencies:
   ```bash
   apt-get update && apt-get install -y python3 python3-pip nodejs npm git curl
   npm install -g @anthropic-ai/claude-code@latest
   ```
3. ECS Console > Instances > Create Custom Image
4. Note the **Image ID** (e.g. `m-bp1xxxxx`)

```yaml
provider:
  aliyun:
    image_id: "m-bp1xxxxx"
```

## 6. Environment Variables

Set your RAM sub-account credentials:

```bash
export ALICLOUD_ACCESS_KEY_ID="LTAI5t..."
export ALICLOUD_ACCESS_KEY_SECRET="HBYwH..."
```

## 7. Final config.yaml Snippet

```yaml
provider:
  type: "aliyun"
  aliyun:
    region_id: "cn-hangzhou"
    image_id: "m-bp1xxxxx"           # Custom image or public Ubuntu image
    instance_type: "ecs.c6.large"
    security_group_id: "sg-bp1xxxxx"
    vswitch_id: "vsw-bp1xxxxx"
    key_pair_name: "elastic-agent-key"
    ssh_key_path: "~/.ssh/elastic-agent-aliyun.pem"
    max_instances: 30
    spot_enabled: false
```

## 8. Validation

Run the validation script to verify all resources are properly configured:

```bash
python scripts/validate_aliyun.py --config config.yaml
```

The script checks:
- Credentials are valid (DescribeRegions API)
- VSwitch exists and is available
- Security Group exists with correct rules
- Key Pair exists
- Image exists (if custom image configured)
