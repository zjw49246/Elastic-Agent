#!/usr/bin/env python3
"""Validate AWS prerequisites for Elastic-Agent.

T-006: Checks VPC/Subnet, Security Group rules, Key Pair, and optional custom AMI.
Usage: python scripts/validate_aws.py --config config.yaml
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

OK = "\033[92m[OK]\033[0m"
FAIL = "\033[91m[FAIL]\033[0m"
WARN = "\033[93m[WARN]\033[0m"
INFO = "\033[94m[INFO]\033[0m"


def _load_config(path: str) -> dict:
    import yaml

    with open(path) as f:
        return yaml.safe_load(f) or {}


def _get_aws_config(config: dict) -> dict:
    return config.get("provider", {}).get("aws", {})


def validate_credentials(session, region: str) -> bool:
    try:
        sts = session.client("sts", region_name=region)
        identity = sts.get_caller_identity()
        account = identity["Account"]
        arn = identity["Arn"]
        print(f"  {OK} Credentials valid (account={account}, arn={arn})")
        return True
    except Exception as e:
        print(f"  {FAIL} Credentials invalid: {e}")
        return False


def validate_subnet(ec2, subnet_id: str) -> bool:
    if not subnet_id:
        print(f"  {FAIL} subnet_id not configured")
        return False

    try:
        resp = ec2.describe_subnets(SubnetIds=[subnet_id])
        subnets = resp.get("Subnets", [])
        if not subnets:
            print(f"  {FAIL} Subnet {subnet_id} not found")
            return False
        sn = subnets[0]
        state = sn["State"]
        az = sn["AvailabilityZone"]
        auto_public = sn.get("MapPublicIpOnLaunch", False)
        print(f"  {OK} Subnet {subnet_id} exists (zone={az}, state={state})")
        if auto_public:
            print(f"  {OK}   Auto-assign public IP enabled")
        else:
            print(f"  {WARN}   Auto-assign public IP disabled — Workers may need public IPs for outbound access")
        return state == "available"
    except Exception as e:
        print(f"  {FAIL} Subnet check failed: {e}")
        return False


def validate_security_group(ec2, sg_ids: list[str]) -> bool:
    if not sg_ids:
        print(f"  {FAIL} security_group_ids not configured")
        return False

    try:
        resp = ec2.describe_security_groups(GroupIds=sg_ids)
        groups = resp.get("SecurityGroups", [])
        if not groups:
            print(f"  {FAIL} Security Groups {sg_ids} not found")
            return False

        for sg in groups:
            sg_id = sg["GroupId"]
            rules = sg.get("IpPermissions", [])
            print(f"  {OK} Security Group {sg_id} exists ({len(rules)} inbound rules)")

            has_ssh = False
            has_runtime = False
            for rule in rules:
                from_port = rule.get("FromPort", 0)
                to_port = rule.get("ToPort", 0)
                if from_port <= 22 <= to_port:
                    has_ssh = True
                if from_port <= 8080 <= to_port:
                    has_runtime = True

            if has_ssh:
                print(f"  {OK}   Port 22 (SSH) allowed")
            else:
                print(f"  {WARN}   Port 22 (SSH) rule not found — Bootstrap needs SSH access")
            if has_runtime:
                print(f"  {OK}   Port 8080 (Runtime) allowed")
            else:
                print(f"  {WARN}   Port 8080 (Runtime) rule not found — Worker Runtime needs this port")

        return True
    except Exception as e:
        print(f"  {FAIL} Security Group check failed: {e}")
        return False


def validate_key_pair(ec2, key_name: str, ssh_key_path: str) -> bool:
    if not key_name:
        print(f"  {FAIL} key_pair_name not configured")
        return False

    try:
        resp = ec2.describe_key_pairs(KeyNames=[key_name])
        pairs = resp.get("KeyPairs", [])
        if not pairs:
            print(f"  {FAIL} Key Pair '{key_name}' not found")
            return False
        kp = pairs[0]
        print(f"  {OK} Key Pair '{key_name}' exists (fingerprint={kp.get('KeyFingerprint', 'N/A')})")
    except Exception as e:
        if "InvalidKeyPair.NotFound" in str(e):
            print(f"  {FAIL} Key Pair '{key_name}' not found")
            return False
        print(f"  {FAIL} Key Pair check failed: {e}")
        return False

    expanded_path = Path(ssh_key_path).expanduser()
    if expanded_path.exists():
        mode = oct(expanded_path.stat().st_mode)[-3:]
        if mode == "600":
            print(f"  {OK} SSH key file exists at {expanded_path} (mode={mode})")
        else:
            print(f"  {WARN} SSH key file at {expanded_path} has mode={mode} (should be 600)")
    else:
        print(f"  {FAIL} SSH key file not found at {expanded_path}")
        return False

    return True


def validate_ami(ec2, ami_id: str) -> bool:
    if not ami_id:
        print(f"  {INFO} No custom ami_id configured (will use public Ubuntu AMI)")
        return True

    try:
        resp = ec2.describe_images(ImageIds=[ami_id])
        images = resp.get("Images", [])
        if not images:
            print(f"  {FAIL} AMI {ami_id} not found")
            return False
        img = images[0]
        state = img["State"]
        name = img.get("Name", "N/A")
        print(f"  {OK} AMI {ami_id} exists (name={name}, state={state})")
        return state == "available"
    except Exception as e:
        print(f"  {FAIL} AMI check failed: {e}")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate AWS prerequisites for Elastic-Agent")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    args = parser.parse_args()

    try:
        import yaml  # noqa: F401
    except ImportError:
        print(f"{FAIL} pyyaml not installed. Run: pip install pyyaml")
        return 1

    try:
        import boto3
    except ImportError:
        print(f"{FAIL} boto3 not installed. Run: pip install 'elastic-agent[aws]'")
        return 1

    config = _load_config(args.config)
    aws_cfg = _get_aws_config(config)
    region = aws_cfg.get("region", "ap-northeast-1")

    session = boto3.Session(region_name=region)
    ec2 = session.client("ec2")

    print(f"\n{'='*60}")
    print(f"Elastic-Agent AWS Prerequisites Validation")
    print(f"Region: {region}")
    print(f"{'='*60}\n")

    results = []

    print("1. Credentials")
    results.append(validate_credentials(session, region))

    print("\n2. Subnet")
    results.append(validate_subnet(ec2, aws_cfg.get("subnet_id", "")))

    print("\n3. Security Group")
    results.append(validate_security_group(ec2, aws_cfg.get("security_group_ids", [])))

    print("\n4. Key Pair")
    results.append(validate_key_pair(
        ec2,
        aws_cfg.get("key_pair_name", ""),
        aws_cfg.get("ssh_key_path", "~/.ssh/elastic-agent-aws.pem"),
    ))

    print("\n5. Custom AMI")
    results.append(validate_ami(ec2, aws_cfg.get("ami_id", "")))

    print(f"\n{'='*60}")
    passed = sum(results)
    total = len(results)
    if all(results):
        print(f"{OK} All {total} checks passed! Elastic-Agent is ready for AWS.")
    else:
        print(f"{FAIL} {total - passed}/{total} checks failed. Fix the issues above.")
    print(f"{'='*60}\n")

    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
