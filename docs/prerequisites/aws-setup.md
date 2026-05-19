# AWS Prerequisites Setup

> This guide covers the one-time manual setup of VPC, Subnet, Security Group, and Key Pair
> required before running Elastic-Agent with the AWS EC2 provider.

## Prerequisites

- An AWS account with EC2 permissions
- IAM user with `AmazonEC2FullAccess` policy (recommended over root)
- AWS CLI (`aws`) configured, or use the AWS Console

## 1. VPC

You can reuse the default VPC or an existing VPC. To create a new one:

**Console:** VPC Dashboard > Your VPCs > Create VPC

| Parameter | Recommended Value |
|---|---|
| Region | Same as your Manager (e.g. `ap-northeast-1`) |
| CIDR Block | `10.0.0.0/16` |
| Name | `elastic-agent-vpc` |

**CLI:**
```bash
aws ec2 create-vpc \
  --cidr-block "10.0.0.0/16" \
  --tag-specifications 'ResourceType=vpc,Tags=[{Key=Name,Value=elastic-agent-vpc}]' \
  --region ap-northeast-1
```

Note the **VPC ID** (e.g. `vpc-0abcd1234`).

Enable DNS hostname resolution:
```bash
aws ec2 modify-vpc-attribute --vpc-id vpc-0abcd1234 --enable-dns-hostnames
```

## 2. Subnet

Manager and Workers should share a subnet for low-latency communication.

**Console:** VPC Dashboard > Subnets > Create Subnet

| Parameter | Recommended Value |
|---|---|
| VPC | Select VPC from step 1 |
| Availability Zone | Any (e.g. `ap-northeast-1a`) |
| CIDR Block | `10.0.1.0/24` |
| Name | `elastic-agent-subnet` |

**CLI:**
```bash
aws ec2 create-subnet \
  --vpc-id vpc-0abcd1234 \
  --cidr-block "10.0.1.0/24" \
  --availability-zone ap-northeast-1a \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=elastic-agent-subnet}]'
```

Enable auto-assign public IP (Workers need outbound access):
```bash
aws ec2 modify-subnet-attribute --subnet-id subnet-0abcd1234 --map-public-ip-on-launch
```

Note the **Subnet ID** (e.g. `subnet-0abcd1234`):
```yaml
provider:
  aws:
    subnet_id: "subnet-0abcd1234"
```

### Internet Gateway (if new VPC)

If you created a new VPC, attach an Internet Gateway for outbound access:

```bash
aws ec2 create-internet-gateway --tag-specifications 'ResourceType=internet-gateway,Tags=[{Key=Name,Value=elastic-agent-igw}]'
aws ec2 attach-internet-gateway --internet-gateway-id igw-0abcd1234 --vpc-id vpc-0abcd1234

# Add route to the main route table
aws ec2 create-route --route-table-id rtb-xxxxx --destination-cidr-block 0.0.0.0/0 --gateway-id igw-0abcd1234
```

## 3. Security Group

**Console:** EC2 Dashboard > Security Groups > Create Security Group

| Parameter | Value |
|---|---|
| VPC | Select VPC from step 1 |
| Name | `elastic-agent-sg` |
| Description | `Elastic Agent Worker security group` |

**Inbound Rules:**

| Type | Protocol | Port | Source | Description |
|---|---|---|---|---|
| SSH | TCP | 22 | VPC CIDR (`10.0.0.0/16`) | SSH for Bootstrap |
| Custom TCP | TCP | 8080 | VPC CIDR (`10.0.0.0/16`) | Worker Runtime |

**CLI:**
```bash
# Create security group
aws ec2 create-security-group \
  --group-name "elastic-agent-sg" \
  --description "Elastic Agent Worker security group" \
  --vpc-id vpc-0abcd1234

# Add SSH rule (VPC internal)
aws ec2 authorize-security-group-ingress \
  --group-id sg-0abcd1234 \
  --protocol tcp --port 22 \
  --cidr "10.0.0.0/16"

# Add Runtime port rule (VPC internal)
aws ec2 authorize-security-group-ingress \
  --group-id sg-0abcd1234 \
  --protocol tcp --port 8080 \
  --cidr "10.0.0.0/16"
```

Note the **Security Group ID** (e.g. `sg-0abcd1234`):
```yaml
provider:
  aws:
    security_group_ids: ["sg-0abcd1234"]
```

## 4. Key Pair

**Console:** EC2 Dashboard > Key Pairs > Create Key Pair

| Parameter | Value |
|---|---|
| Name | `elastic-agent-key` |
| Type | RSA |
| Format | .pem |

Download and save the private key:
```bash
mv elastic-agent-key.pem ~/.ssh/elastic-agent-aws.pem
chmod 600 ~/.ssh/elastic-agent-aws.pem
```

**CLI:**
```bash
aws ec2 create-key-pair \
  --key-name "elastic-agent-key" \
  --query 'KeyMaterial' \
  --output text > ~/.ssh/elastic-agent-aws.pem
chmod 600 ~/.ssh/elastic-agent-aws.pem
```

Config:
```yaml
provider:
  aws:
    key_pair_name: "elastic-agent-key"
    ssh_key_path: "~/.ssh/elastic-agent-aws.pem"
```

## 5. Custom AMI (Optional)

Pre-installing dependencies into an AMI speeds up Bootstrap:

1. Launch a base Ubuntu 22.04 instance
2. Install dependencies:
   ```bash
   apt-get update && apt-get install -y python3 python3-pip nodejs npm git curl
   npm install -g @anthropic-ai/claude-code@latest
   ```
3. EC2 Console > Instances > Actions > Image > Create Image
4. Note the **AMI ID** (e.g. `ami-0abcd1234`)

```yaml
provider:
  aws:
    ami_id: "ami-0abcd1234"
```

## 6. Environment Variables

Set your IAM user credentials:

```bash
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="wJalr..."
# Optional for STS temporary credentials:
# export AWS_SESSION_TOKEN="..."
```

## 7. Final config.yaml Snippet

```yaml
provider:
  type: "aws"
  aws:
    region: "ap-northeast-1"
    ami_id: "ami-0abcd1234"
    default_instance_type: "t3.large"
    security_group_ids: ["sg-0abcd1234"]
    subnet_id: "subnet-0abcd1234"
    key_pair_name: "elastic-agent-key"
    ssh_key_path: "~/.ssh/elastic-agent-aws.pem"
    max_instances: 30
```

## 8. Validation

Run the validation script to verify all resources:

```bash
python scripts/validate_aws.py --config config.yaml
```

The script checks:
- Credentials are valid (DescribRegions / STS GetCallerIdentity)
- Subnet exists and is available
- Security Group exists with correct rules
- Key Pair exists
- AMI exists (if custom AMI configured)
