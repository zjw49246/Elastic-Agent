#!/usr/bin/env python3
"""Validate Aliyun prerequisites for Elastic-Agent.

T-005: Checks VPC/VSwitch, Security Group rules, Key Pair, and optional custom image.
Usage: python scripts/validate_aliyun.py --config config.yaml
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


def _get_aliyun_config(config: dict) -> dict:
    return config.get("provider", {}).get("aliyun", {})


def validate_credentials(client) -> bool:
    from alibabacloud_ecs20140526.models import DescribeRegionsRequest

    try:
        req = DescribeRegionsRequest()
        resp = client.describe_regions(req)
        region_count = len(resp.body.regions.region) if resp.body.regions else 0
        print(f"  {OK} Credentials valid ({region_count} regions accessible)")
        return True
    except Exception as e:
        print(f"  {FAIL} Credentials invalid: {e}")
        return False


def validate_vswitch(client, vswitch_id: str, region_id: str) -> bool:
    from alibabacloud_ecs20140526.models import DescribeVSwitchesRequest

    if not vswitch_id:
        print(f"  {FAIL} vswitch_id not configured")
        return False

    try:
        req = DescribeVSwitchesRequest(region_id=region_id, v_switch_id=vswitch_id)
        resp = client.describe_v_switches(req)
        vswitches = resp.body.v_switches.v_switch if resp.body.v_switches else []
        if not vswitches:
            print(f"  {FAIL} VSwitch {vswitch_id} not found")
            return False
        vs = vswitches[0]
        status = vs.status
        if status == "Available":
            print(f"  {OK} VSwitch {vswitch_id} exists (zone={vs.zone_id}, status={status})")
            return True
        else:
            print(f"  {WARN} VSwitch {vswitch_id} exists but status={status}")
            return False
    except Exception as e:
        print(f"  {FAIL} VSwitch check failed: {e}")
        return False


def validate_security_group(client, sg_id: str, region_id: str) -> bool:
    from alibabacloud_ecs20140526.models import DescribeSecurityGroupAttributeRequest

    if not sg_id:
        print(f"  {FAIL} security_group_id not configured")
        return False

    try:
        req = DescribeSecurityGroupAttributeRequest(
            security_group_id=sg_id, region_id=region_id
        )
        resp = client.describe_security_group_attribute(req)
        rules = resp.body.permissions.permission if resp.body.permissions else []

        has_ssh = False
        has_runtime = False
        for rule in rules:
            if rule.ip_protocol and rule.ip_protocol.upper() == "TCP" and rule.direction == "ingress":
                port_range = rule.port_range or ""
                if "22/22" in port_range or "22" == port_range:
                    has_ssh = True
                if "8080/8080" in port_range or "8080" == port_range:
                    has_runtime = True

        print(f"  {OK} Security Group {sg_id} exists ({len(rules)} rules)")
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


def validate_key_pair(client, key_name: str, region_id: str, ssh_key_path: str) -> bool:
    from alibabacloud_ecs20140526.models import DescribeKeyPairsRequest

    if not key_name:
        print(f"  {FAIL} key_pair_name not configured")
        return False

    try:
        req = DescribeKeyPairsRequest(region_id=region_id, key_pair_name=key_name)
        resp = client.describe_key_pairs(req)
        pairs = resp.body.key_pairs.key_pair if resp.body.key_pairs else []
        if not pairs:
            print(f"  {FAIL} Key Pair '{key_name}' not found")
            return False
        print(f"  {OK} Key Pair '{key_name}' exists (fingerprint={pairs[0].key_pair_finger_print})")
    except Exception as e:
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


def validate_image(client, image_id: str, region_id: str) -> bool:
    from alibabacloud_ecs20140526.models import DescribeImagesRequest

    if not image_id:
        print(f"  {INFO} No custom image_id configured (will use public image)")
        return True

    try:
        req = DescribeImagesRequest(region_id=region_id, image_id=image_id)
        resp = client.describe_images(req)
        images = resp.body.images.image if resp.body.images else []
        if not images:
            print(f"  {FAIL} Image {image_id} not found")
            return False
        img = images[0]
        print(f"  {OK} Image {image_id} exists (name={img.image_name}, status={img.status})")
        return True
    except Exception as e:
        print(f"  {FAIL} Image check failed: {e}")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Aliyun prerequisites for Elastic-Agent")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    args = parser.parse_args()

    try:
        import yaml  # noqa: F401
    except ImportError:
        print(f"{FAIL} pyyaml not installed. Run: pip install pyyaml")
        return 1

    try:
        from alibabacloud_ecs20140526.client import Client
        from alibabacloud_tea_openapi.models import Config as OpenApiConfig
    except ImportError:
        print(f"{FAIL} Aliyun SDK not installed. Run: pip install 'elastic-agent[aliyun]'")
        return 1

    config = _load_config(args.config)
    aliyun_cfg = _get_aliyun_config(config)
    region_id = aliyun_cfg.get("region_id", "cn-hangzhou")

    import os

    access_key_id = os.environ.get("ALICLOUD_ACCESS_KEY_ID", "")
    access_key_secret = os.environ.get("ALICLOUD_ACCESS_KEY_SECRET", "")

    if not access_key_id or not access_key_secret:
        print(f"{FAIL} Set ALICLOUD_ACCESS_KEY_ID and ALICLOUD_ACCESS_KEY_SECRET environment variables")
        return 1

    api_config = OpenApiConfig(
        access_key_id=access_key_id,
        access_key_secret=access_key_secret,
        region_id=region_id,
    )
    api_config.endpoint = f"ecs.{region_id}.aliyuncs.com"
    client = Client(api_config)

    print(f"\n{'='*60}")
    print(f"Elastic-Agent Aliyun Prerequisites Validation")
    print(f"Region: {region_id}")
    print(f"{'='*60}\n")

    results = []

    print("1. Credentials")
    results.append(validate_credentials(client))

    print("\n2. VSwitch")
    results.append(validate_vswitch(client, aliyun_cfg.get("vswitch_id", ""), region_id))

    print("\n3. Security Group")
    results.append(validate_security_group(client, aliyun_cfg.get("security_group_id", ""), region_id))

    print("\n4. Key Pair")
    results.append(validate_key_pair(
        client,
        aliyun_cfg.get("key_pair_name", ""),
        region_id,
        aliyun_cfg.get("ssh_key_path", "~/.ssh/elastic-agent-aliyun.pem"),
    ))

    print("\n5. Custom Image")
    results.append(validate_image(client, aliyun_cfg.get("image_id", ""), region_id))

    print(f"\n{'='*60}")
    passed = sum(results)
    total = len(results)
    if all(results):
        print(f"{OK} All {total} checks passed! Elastic-Agent is ready for Aliyun.")
    else:
        print(f"{FAIL} {total - passed}/{total} checks failed. Fix the issues above.")
    print(f"{'='*60}\n")

    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
