# Elastic-Agent MVP 详细实现计划

> 本文档是 [elastic-agent-analysis.md](elastic-agent-analysis.md) 中 MVP 方案的详细展开，包含完整的 TODO 清单、模块实现细节、测试计划。
>
> **核心策略：** 阿里云优先、SDK 直连管理实例、Terraform 管理基础网络、外部服务 API 暴露实时数据。

---

## TODO 清单

### P0 — 必须完成（框架无法运行的前置条件）

- [ ] **T-001** 项目脚手架搭建（pyproject.toml、目录结构、CI 基础）
- [ ] **T-002** CloudProvider 抽象基类 + Instance 数据模型
- [ ] **T-003** 阿里云 ECS Provider 实现（alibabacloud SDK 直连）
- [ ] **T-004** AWS EC2 Provider 实现（boto3 SDK 直连）
- [ ] **T-005** Terraform 基础网络模块 — 阿里云（VPC/VSwitch/安全组/密钥对）
- [ ] **T-006** Terraform 基础网络模块 — AWS（VPC/Subnet/Security Group/Key Pair）
- [ ] **T-007** Worker Runtime 服务（Worker 侧：接收命令、执行进程、流式日志）
- [ ] **T-008** Worker Runtime 客户端（Manager 侧：远程调用 Worker Runtime）
- [ ] **T-009** Manager ↔ Worker 通信协议定义（WebSocket 反向连接）
- [ ] **T-010** Manager ↔ Worker 认证（共享 Secret Bearer Token）
- [ ] **T-011** 节点注册表（NodeRegistry，JSON/YAML 文件存储）
- [ ] **T-012** 云端标签对账（启动时 + 定期扫描 ManagedBy 标签，清理孤儿实例）
- [ ] **T-013** 外部服务 API — 实时轨迹流（WebSocket/SSE 推送 Agent 轨迹/chat 记录）
- [ ] **T-014** 外部服务 API — 文件传输（Worker 文件读取 + 文件变更监听）
- [ ] **T-015** 外部服务 API — 认证（API Key Bearer Token）
- [ ] **T-016** Manager FastAPI 服务骨架 + 节点管理 REST API
- [ ] **T-017** Claude Code AgentType 实现（安装/启动/健康检查命令）

### P1 — 应该完成（MVP 可用性和稳定性）

- [ ] **T-018** Bootstrap Pipeline 框架（可插拔步骤、超时控制、失败策略）
- [ ] **T-019** Bootstrap 步骤：系统基础初始化（Node.js、Python、uv）
- [ ] **T-020** Bootstrap 步骤：Claude Code 安装与凭证注入
- [ ] **T-021** Bootstrap 步骤：Worker Runtime 部署与启动
- [ ] **T-022** Bootstrap 步骤：Harness 代码部署（git clone）
- [ ] **T-023** Bootstrap 失败处理（terminate + retry / retry from failed / leave for debug）
- [ ] **T-024** Worker 应用级健康检查（L2 Worker Runtime + L3 Agent 进程）
- [ ] **T-025** 优雅缩容 Drain 机制（标记 draining → 等待任务完成 → 终止）
- [ ] **T-026** 凭证分发（API Key 方式，通过 Bootstrap 注入环境变量）
- [ ] **T-027** 基础额度监控（轮询 Worker 上报的 quota 状态）
- [ ] **T-028** 手动扩缩容 API（scale_out / scale_in / remove_node）
- [ ] **T-029** 基础 Web UI（节点列表、状态、手动操作按钮）

### 测试

- [ ] **T-100** 单元测试：CloudProvider 抽象 + Mock Provider
- [ ] **T-101** 单元测试：阿里云 ECS Provider（mock SDK responses）
- [ ] **T-102** 单元测试：AWS EC2 Provider（mock boto3 responses）
- [ ] **T-103** 单元测试：NodeRegistry CRUD 操作
- [ ] **T-104** 单元测试：Worker Runtime 协议消息序列化/反序列化
- [ ] **T-105** 单元测试：Bootstrap Pipeline 步骤执行 + 失败回滚
- [ ] **T-106** 单元测试：Drain 机制状态机
- [ ] **T-107** 单元测试：云端标签对账逻辑
- [ ] **T-108** 单元测试：外部服务 API 轨迹流过滤/推送
- [ ] **T-109** 单元测试：外部服务 API 文件读取/监听
- [ ] **T-110** 集成测试：Manager ↔ Worker Runtime WebSocket 通信
- [ ] **T-111** 集成测试：阿里云 ECS 实例创建/启动/停止/释放全流程
- [ ] **T-112** 集成测试：AWS EC2 实例创建/启动/停止/终止全流程
- [ ] **T-113** 集成测试：Bootstrap Pipeline 端到端（阿里云 ECS 实例）
- [ ] **T-114** 集成测试：外部服务 API 端到端（轨迹流 + 文件传输）
- [ ] **T-115** 集成测试：扩容 → Bootstrap → 执行任务 → 缩容 全链路
- [ ] **T-116** Terraform 测试：阿里云网络模块 plan + apply + destroy
- [ ] **T-117** Terraform 测试：AWS 网络模块 plan + apply + destroy
- [ ] **T-118** DryRunProvider 空跑测试（验证流程不消耗资源）

---

## 1. 项目结构

```
elastic-agent/
├── src/
│   ├── elastic_agent/              # Python 包
│   │   ├── __init__.py             # 公开 API（ElasticAgentManager, Providers, ...）
│   │   ├── core/
│   │   │   ├── providers/
│   │   │   │   ├── base.py         # CloudProvider ABC + Instance 模型
│   │   │   │   ├── aliyun_ecs.py   # 阿里云 ECS Provider（MVP 首选）
│   │   │   │   ├── aws_ec2.py      # AWS EC2 Provider
│   │   │   │   └── dry_run.py      # DryRun Provider（测试用）
│   │   │   ├── agents/
│   │   │   │   ├── base.py         # AgentType ABC
│   │   │   │   └── claude_code.py  # Claude Code 实现
│   │   │   ├── credentials/
│   │   │   │   ├── pool.py         # CredentialPool（账号池）
│   │   │   │   ├── provider.py     # CredentialProvider ABC
│   │   │   │   └── api_key.py      # API Key 分发（MVP）
│   │   │   ├── runtime/
│   │   │   │   ├── protocol.py     # Manager↔Worker 通信协议
│   │   │   │   ├── client.py       # Manager 侧 — 远程调用 Worker
│   │   │   │   └── server.py       # Worker 侧 — 接收命令、执行进程
│   │   │   ├── bootstrap/
│   │   │   │   ├── pipeline.py     # 可插拔初始化管道
│   │   │   │   ├── policy.py       # 失败处理策略
│   │   │   │   └── steps/          # 内置步骤（系统初始化、Agent 安装等）
│   │   │   ├── registry/
│   │   │   │   └── store.py        # NodeRegistry（JSON 文件存储）
│   │   │   ├── monitor/
│   │   │   │   ├── health.py       # L2/L3 健康检查
│   │   │   │   ├── quota.py        # 额度监控
│   │   │   │   ├── reconciler.py   # 云端标签对账
│   │   │   │   └── events.py       # 事件总线
│   │   │   ├── scheduler/
│   │   │   │   └── drain.py        # 优雅缩容 Drain 机制
│   │   │   ├── external_api/
│   │   │   │   ├── traces.py       # 实时轨迹流（WebSocket/SSE）
│   │   │   │   ├── files.py        # 文件传输（读取 + 监听）
│   │   │   │   ├── auth.py         # 外部 API 认证
│   │   │   │   └── router.py       # FastAPI Router 挂载点
│   │   │   └── security/
│   │   │       └── auth.py         # Manager↔Worker Bearer Token 认证
│   │   ├── manager/
│   │   │   ├── api/
│   │   │   │   ├── nodes.py        # 节点管理 API
│   │   │   │   ├── credentials.py  # 凭证管理 API
│   │   │   │   └── status.py       # 集群状态 API
│   │   │   ├── service.py          # ElasticAgentManager 主类
│   │   │   └── config.py           # Pydantic 配置模型
│   │   ├── worker/
│   │   │   ├── runtime_server.py   # Worker Runtime HTTP/WS 服务入口
│   │   │   ├── process_manager.py  # 本地进程管理
│   │   │   ├── file_watcher.py     # 文件变更监听（inotify）
│   │   │   └── reporter.py         # 状态/日志上报
│   │   └── cli/
│   │       └── main.py             # 命令行入口
├── dashboard/                      # 前端 UI（React + Vite + Ant Design）
├── infra/                          # Terraform IaC
│   ├── modules/
│   │   ├── networking/
│   │   │   ├── main.tf             # VPC、子网、路由表、NAT
│   │   │   ├── security_groups.tf  # 安全组规则
│   │   │   ├── variables.tf
│   │   │   └── outputs.tf          # vpc_id, subnet_ids, sg_ids
│   │   └── base/
│   │       ├── main.tf             # 密钥对、IAM/RAM 角色
│   │       ├── variables.tf
│   │       └── outputs.tf
│   └── environments/
│       ├── aliyun-cn-hangzhou/     # 阿里云杭州（MVP 首选）
│       │   ├── main.tf
│       │   ├── terraform.tfvars
│       │   └── backend.tf          # OSS remote state
│       └── aws-ap-northeast-1/     # AWS 东京
│           ├── main.tf
│           ├── terraform.tfvars
│           └── backend.tf          # S3 remote state
├── tests/
│   ├── unit/
│   │   ├── test_providers.py
│   │   ├── test_registry.py
│   │   ├── test_protocol.py
│   │   ├── test_bootstrap.py
│   │   ├── test_drain.py
│   │   ├── test_reconciler.py
│   │   ├── test_external_traces.py
│   │   └── test_external_files.py
│   └── integration/
│       ├── test_aliyun_e2e.py
│       ├── test_aws_e2e.py
│       ├── test_runtime_ws.py
│       ├── test_external_api_e2e.py
│       └── test_full_lifecycle.py
├── scripts/
│   ├── bootstrap.sh                # Worker 初始化脚本
│   └── watchdog.sh                 # Worker 侧 watchdog
├── examples/
│   ├── claude-code-manager/
│   └── agent-ml-research/
├── pyproject.toml
├── CLAUDE.md
└── README.md
```

---

## 2. 模块实现详解

### 2.1 CloudProvider 抽象 + 数据模型 (T-002)

```python
# src/elastic_agent/core/providers/base.py

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

class InstanceStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    TERMINATING = "terminating"
    TERMINATED = "terminated"

@dataclass
class InstanceConfig:
    name: str
    instance_type: str | None = None    # 不指定则用 Provider 默认值
    image_id: str | None = None         # 不指定则用 Provider 默认值
    tags: dict[str, str] = field(default_factory=dict)
    subnet_id: str | None = None       # VSwitch ID (阿里云) / Subnet ID (AWS)
    security_group_ids: list[str] = field(default_factory=list)
    key_pair_name: str | None = None
    user_data: str | None = None        # cloud-init 脚本
    spot: bool = False                  # 是否使用抢占式/Spot 实例

@dataclass
class Instance:
    id: str                             # 实例 ID（阿里云 i-xxx / AWS i-xxx）
    name: str
    status: InstanceStatus
    public_ip: str | None = None
    private_ip: str | None = None
    instance_type: str = ""
    region: str = ""
    zone: str = ""
    created_at: datetime | None = None
    tags: dict[str, str] = field(default_factory=dict)
    provider: str = ""                  # "aliyun" / "aws"
    raw: dict[str, Any] = field(default_factory=dict)  # 云厂商原始响应

class CloudProvider(ABC):
    """云服务商接口 — 管理实例生命周期"""

    @abstractmethod
    async def create_instance(self, config: InstanceConfig) -> Instance: ...

    @abstractmethod
    async def start_instance(self, instance_id: str) -> None: ...

    @abstractmethod
    async def stop_instance(self, instance_id: str) -> None: ...

    @abstractmethod
    async def terminate_instance(self, instance_id: str) -> None: ...

    @abstractmethod
    async def list_instances(self, filters: dict | None = None) -> list[Instance]: ...

    @abstractmethod
    async def get_instance(self, instance_id: str) -> Instance: ...

    @abstractmethod
    async def wait_until_running(self, instance_id: str, timeout: int = 300) -> Instance: ...
```

### 2.2 阿里云 ECS Provider (T-003)

**MVP 首选实现。** 基于 alibabacloud_ecs20140526 SDK V2.0。

```python
# src/elastic_agent/core/providers/aliyun_ecs.py

from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_tea_openapi import models as open_api_models

class AliyunEcsProvider(CloudProvider):
    def __init__(self, config: AliyunEcsConfig):
        self.config = config
        api_config = open_api_models.Config(
            access_key_id=config.access_key_id,
            access_key_secret=config.access_key_secret,
            region_id=config.region_id,
        )
        self.client = EcsClient(api_config)

    async def create_instance(self, config: InstanceConfig) -> Instance:
        # 构建 Tag 列表（必须包含 ManagedBy=elastic-agent）
        tags = [
            ecs_models.RunInstancesRequestTag(
                key="ManagedBy", value="elastic-agent"
            ),
            ecs_models.RunInstancesRequestTag(
                key="Name", value=f"Worker-{config.name}"
            ),
        ]
        for k, v in config.tags.items():
            tags.append(ecs_models.RunInstancesRequestTag(key=k, value=v))

        request = ecs_models.RunInstancesRequest(
            region_id=self.config.region_id,
            image_id=config.image_id or self.config.image_id,
            instance_type=config.instance_type or self.config.instance_type,
            security_group_id=self.config.security_group_id,
            v_switch_id=config.subnet_id or self.config.vswitch_id,
            key_pair_name=config.key_pair_name or self.config.key_pair_name,
            internet_charge_type=self.config.internet_charge_type,
            internet_max_bandwidth_out=self.config.internet_max_bandwidth_out,
            system_disk=ecs_models.RunInstancesRequestSystemDisk(
                category=self.config.system_disk_category,
                size=self.config.system_disk_size,
            ),
            amount=1,
            tag=tags,
            # 抢占式实例配置
            spot_strategy="SpotAsPriceGo" if config.spot else "NoSpot",
            instance_name=f"Worker-{config.name}",
        )

        response = self.client.run_instances(request)
        instance_id = response.body.instance_id_sets.instance_id_set[0]
        return await self.get_instance(instance_id)

    async def terminate_instance(self, instance_id: str) -> None:
        # 阿里云需要先停止再释放（或 Force=True）
        request = ecs_models.DeleteInstanceRequest(
            instance_id=instance_id,
            force=True,
        )
        self.client.delete_instance(request)

    async def list_instances(self, filters: dict | None = None) -> list[Instance]:
        # 默认过滤 ManagedBy=elastic-agent 的实例
        tag = [ecs_models.DescribeInstancesRequestTag(
            key="ManagedBy", value="elastic-agent"
        )]
        request = ecs_models.DescribeInstancesRequest(
            region_id=self.config.region_id,
            tag=tag,
            page_size=100,
        )
        response = self.client.describe_instances(request)
        return [self._to_instance(i) for i in response.body.instances.instance]

    async def wait_until_running(self, instance_id: str, timeout: int = 300) -> Instance:
        import asyncio
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            inst = await self.get_instance(instance_id)
            if inst.status == InstanceStatus.RUNNING and inst.public_ip:
                return inst
            await asyncio.sleep(5)
        raise TimeoutError(f"Instance {instance_id} not running after {timeout}s")

    def _to_instance(self, raw) -> Instance:
        """将阿里云 API 响应转换为统一 Instance 模型"""
        public_ip = None
        if raw.public_ip_address and raw.public_ip_address.ip_address:
            public_ip = raw.public_ip_address.ip_address[0]
        elif raw.eip_address and raw.eip_address.ip_address:
            public_ip = raw.eip_address.ip_address

        status_map = {
            "Pending": InstanceStatus.PENDING,
            "Running": InstanceStatus.RUNNING,
            "Stopping": InstanceStatus.STOPPING,
            "Stopped": InstanceStatus.STOPPED,
        }

        return Instance(
            id=raw.instance_id,
            name=raw.instance_name or "",
            status=status_map.get(raw.status, InstanceStatus.PENDING),
            public_ip=public_ip,
            private_ip=raw.vpc_attributes.private_ip_address.ip_address[0]
                if raw.vpc_attributes and raw.vpc_attributes.private_ip_address
                else None,
            instance_type=raw.instance_type,
            region=raw.region_id,
            zone=raw.zone_id,
            created_at=raw.creation_time,
            provider="aliyun",
            raw=raw.__dict__ if hasattr(raw, '__dict__') else {},
        )
```

#### 阿里云配置模型

```python
class AliyunEcsConfig(BaseModel):
    access_key_id: str
    access_key_secret: str
    region_id: str = "cn-hangzhou"
    image_id: str = ""                          # 自定义镜像（预装 Ubuntu + 基础工具）
    instance_type: str = "ecs.c6.large"         # 2 vCPU / 4 GiB
    security_group_id: str = ""                 # 从 Terraform 输出获取
    vswitch_id: str = ""                        # 从 Terraform 输出获取
    key_pair_name: str = "elastic-agent-key"    # 从 Terraform 创建
    ssh_key_path: str = "~/.ssh/elastic-agent.pem"
    ssh_user: str = "root"
    max_instances: int = 30
    internet_charge_type: str = "PayByTraffic"
    internet_max_bandwidth_out: int = 100
    system_disk_category: str = "cloud_essd"
    system_disk_size: int = 40
    spot_strategy: str = "NoSpot"
```

### 2.3 AWS EC2 Provider (T-004)

同步实现，验证 CloudProvider 抽象的正确性。

```python
# src/elastic_agent/core/providers/aws_ec2.py

import boto3

class AWSEc2Provider(CloudProvider):
    def __init__(self, config: AWSEc2Config):
        self.config = config
        self.ec2 = boto3.client('ec2', region_name=config.region)

    async def create_instance(self, config: InstanceConfig) -> Instance:
        tags = [
            {"Key": "ManagedBy", "Value": "elastic-agent"},
            {"Key": "Name", "Value": f"Worker-{config.name}"},
        ]
        for k, v in config.tags.items():
            tags.append({"Key": k, "Value": v})

        resp = self.ec2.run_instances(
            ImageId=config.image_id or self.config.ami_id,
            InstanceType=config.instance_type or self.config.default_instance_type,
            MinCount=1, MaxCount=1,
            KeyName=config.key_pair_name or self.config.key_pair_name,
            SecurityGroupIds=config.security_group_ids or self.config.security_group_ids,
            SubnetId=config.subnet_id or self.config.subnet_id,
            TagSpecifications=[{
                "ResourceType": "instance",
                "Tags": tags,
            }],
            InstanceMarketOptions={
                "MarketType": "spot",
                "SpotOptions": {"SpotInstanceType": "one-time"},
            } if config.spot else {},
        )
        return self._to_instance(resp["Instances"][0])

    async def terminate_instance(self, instance_id: str) -> None:
        self.ec2.terminate_instances(InstanceIds=[instance_id])

    async def list_instances(self, filters: dict | None = None) -> list[Instance]:
        resp = self.ec2.describe_instances(Filters=[
            {"Name": "tag:ManagedBy", "Values": ["elastic-agent"]},
            {"Name": "instance-state-name", "Values": [
                "pending", "running", "stopping", "stopped"
            ]},
        ])
        instances = []
        for reservation in resp["Reservations"]:
            for inst in reservation["Instances"]:
                instances.append(self._to_instance(inst))
        return instances
```

### 2.4 Terraform 基础网络模块 (T-005, T-006)

#### 阿里云网络模块

```hcl
# infra/modules/networking/main.tf (阿里云版本通过 provider 切换)

# infra/environments/aliyun-cn-hangzhou/main.tf
terraform {
  required_providers {
    alicloud = {
      source  = "aliyun/alicloud"
      version = "~> 1.220"
    }
  }
}

provider "alicloud" {
  region = var.region_id
}

# VPC
resource "alicloud_vpc" "elastic_agent" {
  vpc_name   = "elastic-agent-vpc"
  cidr_block = var.vpc_cidr  # 默认 "172.16.0.0/16"
}

# VSwitch（子网）— 至少 2 个用于高可用
resource "alicloud_vswitch" "worker_a" {
  vpc_id       = alicloud_vpc.elastic_agent.id
  cidr_block   = "172.16.1.0/24"
  zone_id      = "${var.region_id}-a"
  vswitch_name = "elastic-agent-worker-a"
}

resource "alicloud_vswitch" "worker_b" {
  vpc_id       = alicloud_vpc.elastic_agent.id
  cidr_block   = "172.16.2.0/24"
  zone_id      = "${var.region_id}-b"
  vswitch_name = "elastic-agent-worker-b"
}

# 安全组
resource "alicloud_security_group" "worker" {
  name        = "elastic-agent-worker-sg"
  vpc_id      = alicloud_vpc.elastic_agent.id
  description = "Security group for Elastic-Agent workers"
}

# 安全组规则 — SSH（仅 Manager IP）
resource "alicloud_security_group_rule" "ssh" {
  type              = "ingress"
  ip_protocol       = "tcp"
  port_range        = "22/22"
  security_group_id = alicloud_security_group.worker.id
  cidr_ip           = var.manager_cidr  # Manager 所在的 CIDR
}

# 安全组规则 — Worker Runtime（VPC 内部）
resource "alicloud_security_group_rule" "worker_runtime" {
  type              = "ingress"
  ip_protocol       = "tcp"
  port_range        = "8080/8080"
  security_group_id = alicloud_security_group.worker.id
  cidr_ip           = var.vpc_cidr
}

# 安全组规则 — 出站全放行
resource "alicloud_security_group_rule" "egress" {
  type              = "egress"
  ip_protocol       = "all"
  port_range        = "-1/-1"
  security_group_id = alicloud_security_group.worker.id
  cidr_ip           = "0.0.0.0/0"
}

# 密钥对
resource "alicloud_ecs_key_pair" "worker" {
  key_pair_name = "elastic-agent-key"
  # public_key 从变量传入，或让阿里云生成
}

# NAT Gateway（Worker 出站 IP 固定）— IP 亲和性基础
resource "alicloud_nat_gateway" "main" {
  vpc_id           = alicloud_vpc.elastic_agent.id
  nat_gateway_name = "elastic-agent-nat"
  payment_type     = "PayAsYouGo"
  vswitch_id       = alicloud_vswitch.worker_a.id
  nat_type         = "Enhanced"
}

resource "alicloud_eip_address" "nat" {
  address_name         = "elastic-agent-nat-eip"
  bandwidth            = 100
  internet_charge_type = "PayByTraffic"
}

resource "alicloud_eip_association" "nat" {
  allocation_id = alicloud_eip_address.nat.id
  instance_id   = alicloud_nat_gateway.main.id
  instance_type = "Nat"
}

resource "alicloud_snat_entry" "worker_a" {
  snat_table_id     = alicloud_nat_gateway.main.snat_table_ids
  source_vswitch_id = alicloud_vswitch.worker_a.id
  snat_ip           = alicloud_eip_address.nat.ip_address
}

# Outputs — Provider 代码从这里读取
output "vpc_id" { value = alicloud_vpc.elastic_agent.id }
output "vswitch_ids" { value = [alicloud_vswitch.worker_a.id, alicloud_vswitch.worker_b.id] }
output "security_group_id" { value = alicloud_security_group.worker.id }
output "key_pair_name" { value = alicloud_ecs_key_pair.worker.key_pair_name }
output "nat_eip" { value = alicloud_eip_address.nat.ip_address }
```

#### AWS 网络模块（结构一致，provider 不同）

```hcl
# infra/environments/aws-ap-northeast-1/main.tf
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

resource "aws_vpc" "elastic_agent" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  tags = { Name = "elastic-agent-vpc" }
}

resource "aws_subnet" "worker_a" {
  vpc_id            = aws_vpc.elastic_agent.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = "${var.region}a"
  tags = { Name = "elastic-agent-worker-a" }
}

resource "aws_security_group" "worker" {
  name        = "elastic-agent-worker-sg"
  vpc_id      = aws_vpc.elastic_agent.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.manager_cidr]
  }

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_key_pair" "worker" {
  key_name   = "elastic-agent-key"
  public_key = var.ssh_public_key
}

# NAT Gateway
resource "aws_eip" "nat" { domain = "vpc" }
resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public.id
}

output "vpc_id" { value = aws_vpc.elastic_agent.id }
output "subnet_ids" { value = [aws_subnet.worker_a.id] }
output "security_group_id" { value = aws_security_group.worker.id }
output "key_pair_name" { value = aws_key_pair.worker.key_name }
```

### 2.5 Worker Runtime (T-007, T-008, T-009)

Worker Runtime 是框架的核心 — 运行在每个 Worker 上，接收 Manager 的命令，执行进程，流式回传日志。

#### 通信协议 (T-009)

```python
# src/elastic_agent/core/runtime/protocol.py

from enum import Enum
from pydantic import BaseModel

class MessageType(str, Enum):
    # Manager → Worker
    EXECUTE = "execute"          # 执行命令
    STOP = "stop"                # 停止进程
    READ_FILE = "read_file"      # 读取文件
    WATCH_FILES = "watch_files"  # 监听文件变化
    HEALTH_CHECK = "health_check"

    # Worker → Manager
    LOG = "log"                  # 日志事件
    STATUS = "status"            # 状态上报
    FILE_CONTENT = "file_content"  # 文件内容
    FILE_CHANGE = "file_change"  # 文件变更事件
    PROCESS_EXIT = "process_exit"  # 进程退出
    HEARTBEAT = "heartbeat"      # 心跳

class ExecuteRequest(BaseModel):
    task_id: str
    command: list[str]
    cwd: str = "/workspace"
    env: dict[str, str] = {}
    timeout: int | None = None

class LogEvent(BaseModel):
    timestamp: str
    task_id: str
    stream: str  # "stdout" / "stderr"
    data: str
    worker_id: str

class FileReadRequest(BaseModel):
    path: str
    encoding: str = "utf-8"

class FileWatchRequest(BaseModel):
    paths: list[str]
    events: list[str] = ["modified", "created", "deleted"]

class FileChangeEvent(BaseModel):
    path: str
    event: str
    content: str | None = None
    timestamp: str
    worker_id: str
```

#### Worker 侧服务 (T-007)

```python
# src/elastic_agent/worker/runtime_server.py

from fastapi import FastAPI, WebSocket
import asyncio

app = FastAPI()
processes: dict[str, asyncio.subprocess.Process] = {}

@app.websocket("/ws/runtime")
async def runtime_websocket(ws: WebSocket):
    """主 WebSocket 通道 — Worker 主动连接 Manager"""
    await ws.accept()
    # 验证 Bearer Token
    auth = await ws.receive_json()
    if not verify_token(auth.get("token")):
        await ws.close(code=4001)
        return

    # 双向消息循环
    while True:
        msg = await ws.receive_json()
        msg_type = msg["type"]

        if msg_type == "execute":
            req = ExecuteRequest(**msg["payload"])
            asyncio.create_task(execute_and_stream(ws, req))

        elif msg_type == "stop":
            task_id = msg["payload"]["task_id"]
            await stop_process(task_id)

        elif msg_type == "read_file":
            req = FileReadRequest(**msg["payload"])
            content = await read_local_file(req.path, req.encoding)
            await ws.send_json({
                "type": "file_content",
                "payload": {"path": req.path, "content": content}
            })

        elif msg_type == "watch_files":
            req = FileWatchRequest(**msg["payload"])
            asyncio.create_task(watch_and_stream(ws, req))

        elif msg_type == "health_check":
            await ws.send_json({
                "type": "status",
                "payload": get_worker_status()
            })

async def execute_and_stream(ws: WebSocket, req: ExecuteRequest):
    """启动子进程并流式回传 stdout/stderr"""
    proc = await asyncio.create_subprocess_exec(
        *req.command,
        cwd=req.cwd,
        env={**os.environ, **req.env},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    processes[req.task_id] = proc

    async def stream_pipe(pipe, stream_name):
        async for line in pipe:
            await ws.send_json({
                "type": "log",
                "payload": {
                    "timestamp": datetime.utcnow().isoformat(),
                    "task_id": req.task_id,
                    "stream": stream_name,
                    "data": line.decode("utf-8", errors="replace"),
                    "worker_id": WORKER_ID,
                }
            })

    await asyncio.gather(
        stream_pipe(proc.stdout, "stdout"),
        stream_pipe(proc.stderr, "stderr"),
    )

    exit_code = await proc.wait()
    del processes[req.task_id]
    await ws.send_json({
        "type": "process_exit",
        "payload": {
            "task_id": req.task_id,
            "exit_code": exit_code,
            "worker_id": WORKER_ID,
        }
    })

async def watch_and_stream(ws: WebSocket, req: FileWatchRequest):
    """使用 inotify 监听文件变化并流式推送"""
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler

    class Handler(FileSystemEventHandler):
        def on_any_event(self, event):
            if event.is_directory:
                return
            content = None
            if event.event_type in ("modified", "created"):
                try:
                    with open(event.src_path, "r") as f:
                        content = f.read()
                except (OSError, UnicodeDecodeError):
                    pass
            asyncio.run_coroutine_threadsafe(
                ws.send_json({
                    "type": "file_change",
                    "payload": {
                        "path": event.src_path,
                        "event": event.event_type,
                        "content": content,
                        "timestamp": datetime.utcnow().isoformat(),
                        "worker_id": WORKER_ID,
                    }
                }),
                loop,
            )

    observer = Observer()
    for path in req.paths:
        observer.schedule(Handler(), path=os.path.dirname(path), recursive=False)
    observer.start()
```

#### Manager 侧客户端 (T-008)

```python
# src/elastic_agent/core/runtime/client.py

import websockets
from typing import AsyncIterator

class WorkerRuntimeClient:
    """Manager 侧 — 与 Worker Runtime 的通信客户端"""

    def __init__(self, worker_url: str, auth_token: str):
        self.url = worker_url
        self.token = auth_token
        self.ws = None
        self._subscribers: dict[str, list[asyncio.Queue]] = {}

    async def connect(self):
        self.ws = await websockets.connect(self.url)
        await self.ws.send(json.dumps({"token": self.token}))
        asyncio.create_task(self._message_loop())

    async def execute(self, task_id: str, command: list[str],
                      cwd: str = "/workspace", env: dict = None) -> AsyncIterator[LogEvent]:
        """在 Worker 上执行命令，返回日志流"""
        queue = asyncio.Queue()
        self._subscribers.setdefault(task_id, []).append(queue)

        await self.ws.send(json.dumps({
            "type": "execute",
            "payload": {
                "task_id": task_id,
                "command": command,
                "cwd": cwd,
                "env": env or {},
            }
        }))

        while True:
            event = await queue.get()
            if event["type"] == "process_exit":
                yield event
                break
            yield event

    async def read_file(self, path: str) -> str:
        """从 Worker 读取文件内容"""
        await self.ws.send(json.dumps({
            "type": "read_file",
            "payload": {"path": path}
        }))
        # 等待响应（通过 _message_loop 分发）
        ...

    async def watch_files(self, paths: list[str]) -> AsyncIterator[FileChangeEvent]:
        """监听 Worker 上的文件变化"""
        await self.ws.send(json.dumps({
            "type": "watch_files",
            "payload": {"paths": paths}
        }))
        queue = asyncio.Queue()
        self._file_watchers.append(queue)
        while True:
            event = await queue.get()
            yield FileChangeEvent(**event["payload"])
```

### 2.6 外部服务 API (T-013, T-014, T-015)

外部服务通过 Manager 暴露的 API 获取实时的 Agent 轨迹和文件。

```python
# src/elastic_agent/core/external_api/router.py

from fastapi import APIRouter, WebSocket, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

external_router = APIRouter(prefix="/api/external", tags=["external"])

# ── 认证中间件 ──

async def verify_api_key(api_key: str = Query(..., alias="api_key")):
    if api_key not in VALID_API_KEYS:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key

# ── 实时轨迹流 ──

@external_router.websocket("/traces/{node_id}/stream")
async def stream_node_traces(ws: WebSocket, node_id: str):
    """WebSocket：实时推送指定 Worker 的 Agent 轨迹/chat 记录"""
    await ws.accept()
    # 认证
    init_msg = await ws.receive_json()
    if not verify_api_key_sync(init_msg.get("api_key")):
        await ws.close(code=4001)
        return
    # 订阅该 Worker 的事件
    async for event in event_bus.subscribe(node_id=node_id, event_types=["log"]):
        await ws.send_json({
            "timestamp": event.timestamp,
            "type": event.payload.get("stream", "stdout"),
            "content": event.payload.get("data", ""),
            "task_id": event.payload.get("task_id"),
            "worker_id": node_id,
        })

@external_router.get("/traces/{node_id}/stream/sse")
async def stream_node_traces_sse(node_id: str, api_key: str = Depends(verify_api_key)):
    """SSE：实时推送（备选方案，浏览器原生支持）"""
    async def event_generator():
        async for event in event_bus.subscribe(node_id=node_id, event_types=["log"]):
            yield f"data: {json.dumps(event.payload)}\n\n"
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@external_router.websocket("/traces/all/stream")
async def stream_all_traces(ws: WebSocket):
    """WebSocket：推送所有 Worker 的 Agent 轨迹（全局监控）"""
    await ws.accept()
    init_msg = await ws.receive_json()
    if not verify_api_key_sync(init_msg.get("api_key")):
        await ws.close(code=4001)
        return
    async for event in event_bus.subscribe(event_types=["log"]):
        await ws.send_json(event.payload)

# ── 历史轨迹查询 ──

@external_router.get("/traces/{node_id}")
async def get_node_traces(
    node_id: str,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 100,
    task_id: str | None = None,
    api_key: str = Depends(verify_api_key),
):
    """REST：查询指定 Worker 的历史轨迹记录"""
    return trace_store.query(
        node_id=node_id, since=since, until=until,
        limit=limit, task_id=task_id,
    )

# ── 文件传输 ──

@external_router.get("/files/{node_id}/{file_path:path}")
async def get_worker_file(
    node_id: str,
    file_path: str,
    api_key: str = Depends(verify_api_key),
):
    """REST：通过 Manager 从 Worker 下载指定文件"""
    runtime_client = get_runtime_client(node_id)
    content = await runtime_client.read_file(f"/{file_path}")
    return {"path": file_path, "content": content, "worker_id": node_id}

@external_router.websocket("/files/{node_id}/watch")
async def watch_worker_files(ws: WebSocket, node_id: str):
    """WebSocket：监听 Worker 上指定文件的变化"""
    await ws.accept()
    init_msg = await ws.receive_json()
    if not verify_api_key_sync(init_msg.get("api_key")):
        await ws.close(code=4001)
        return

    paths = init_msg.get("paths", [])
    runtime_client = get_runtime_client(node_id)
    async for change in runtime_client.watch_files(paths):
        await ws.send_json({
            "path": change.path,
            "event": change.event,
            "content": change.content,
            "timestamp": change.timestamp,
            "worker_id": node_id,
        })

# ── 集群状态 ──

@external_router.get("/cluster/status")
async def get_cluster_status(api_key: str = Depends(verify_api_key)):
    """REST：获取集群整体状态"""
    nodes = await manager.list_nodes()
    return {
        "total_nodes": len(nodes),
        "running": sum(1 for n in nodes if n.status == "running"),
        "idle": sum(1 for n in nodes if n.status == "idle"),
        "nodes": [
            {
                "id": n.id,
                "status": n.status,
                "ip": n.public_ip,
                "provider": n.provider,
                "tasks": n.active_tasks,
            }
            for n in nodes
        ],
    }
```

### 2.7 Manager ↔ Worker 认证 (T-010)

```python
# src/elastic_agent/core/security/auth.py

import secrets

def generate_worker_token() -> str:
    """在 Bootstrap 时为每个 Worker 生成唯一 token"""
    return secrets.token_urlsafe(32)

class WorkerAuthMiddleware:
    """验证 Worker Runtime WebSocket 连接的 token"""

    def __init__(self, registry: NodeRegistry):
        self.registry = registry

    async def verify(self, token: str) -> str | None:
        """验证 token，返回 worker_id 或 None"""
        node = self.registry.get_by_token(token)
        return node.id if node else None
```

### 2.8 节点注册表 (T-011) + 云端对账 (T-012)

```python
# src/elastic_agent/core/registry/store.py

import json
from pathlib import Path
from threading import Lock

class NodeRegistry:
    def __init__(self, path: str = "~/.elastic-agent/registry.json"):
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        self._data: dict[str, dict] = self._load()

    def add(self, instance: Instance, credential_id: str | None = None,
            auth_token: str = "") -> None:
        with self._lock:
            self._data[instance.id] = {
                "instance_id": instance.id,
                "name": instance.name,
                "status": instance.status.value,
                "public_ip": instance.public_ip,
                "private_ip": instance.private_ip,
                "provider": instance.provider,
                "credential_id": credential_id,
                "auth_token": auth_token,
                "created_at": instance.created_at.isoformat() if instance.created_at else None,
                "last_seen": datetime.utcnow().isoformat(),
            }
            self._save()

    def remove(self, instance_id: str) -> None:
        with self._lock:
            self._data.pop(instance_id, None)
            self._save()

    def get_by_token(self, token: str) -> dict | None:
        for node in self._data.values():
            if node.get("auth_token") == token:
                return node
        return None

    def list_all(self) -> list[dict]:
        return list(self._data.values())
```

```python
# src/elastic_agent/core/monitor/reconciler.py

class CloudReconciler:
    """云端标签对账 — 防止孤儿实例"""

    def __init__(self, provider: CloudProvider, registry: NodeRegistry):
        self.provider = provider
        self.registry = registry

    async def reconcile(self):
        """对比云端实例和本地注册表，处理不一致"""
        cloud_instances = await self.provider.list_instances()
        registered_ids = {n["instance_id"] for n in self.registry.list_all()}
        cloud_ids = {i.id for i in cloud_instances}

        # 孤儿实例：云上有但注册表没有
        orphans = cloud_ids - registered_ids
        for orphan_id in orphans:
            inst = next(i for i in cloud_instances if i.id == orphan_id)
            logger.warning(f"Orphan instance found: {orphan_id} ({inst.public_ip})")
            # 策略：纳入管理 或 清理
            if self.config.auto_cleanup_orphans:
                await self.provider.terminate_instance(orphan_id)
                logger.info(f"Orphan instance terminated: {orphan_id}")
            else:
                self.registry.add(inst)  # 纳入管理

        # 幽灵节点：注册表有但云上已消失
        ghosts = registered_ids - cloud_ids
        for ghost_id in ghosts:
            logger.warning(f"Ghost node found: {ghost_id}")
            self.registry.remove(ghost_id)
```

### 2.9 Bootstrap Pipeline (T-018 ~ T-023)

```python
# src/elastic_agent/core/bootstrap/pipeline.py

from enum import Enum

class BootstrapFailurePolicy(Enum):
    TERMINATE_AND_RETRY = "terminate_and_retry"
    RETRY_FROM_FAILED = "retry_from_failed"
    LEAVE_FOR_DEBUG = "leave_for_debug"

class BootstrapPipeline:
    def __init__(self, steps: list[BootstrapStep],
                 failure_policy: BootstrapFailurePolicy = BootstrapFailurePolicy.TERMINATE_AND_RETRY,
                 max_retries: int = 2):
        self.steps = steps
        self.failure_policy = failure_policy
        self.max_retries = max_retries

    async def execute(self, ctx: BootstrapContext) -> bool:
        completed_steps = []
        for step in self.steps:
            try:
                logger.info(f"Bootstrap step [{step.name}] starting...")
                await asyncio.wait_for(
                    step.execute(ctx),
                    timeout=step.timeout if hasattr(step, 'timeout') else 600,
                )
                completed_steps.append(step.name)
                logger.info(f"Bootstrap step [{step.name}] completed")
            except Exception as e:
                logger.error(f"Bootstrap step [{step.name}] failed: {e}")
                ctx.failed_step = step.name
                ctx.error = str(e)
                return False
        return True
```

### 2.10 健康检查 (T-024)

```python
# src/elastic_agent/core/monitor/health.py

class HealthChecker:
    """多层健康检查"""

    async def check_node(self, node: dict, runtime_client: WorkerRuntimeClient) -> HealthReport:
        report = HealthReport(node_id=node["instance_id"])

        # L1: 基础设施 — VM 是否在运行
        try:
            instance = await self.provider.get_instance(node["instance_id"])
            report.l1_infra = instance.status == InstanceStatus.RUNNING
        except Exception:
            report.l1_infra = False
            return report

        # L2: Worker Runtime — 服务是否响应
        try:
            status = await asyncio.wait_for(
                runtime_client.health_check(), timeout=10
            )
            report.l2_runtime = True
            report.runtime_status = status
        except Exception:
            report.l2_runtime = False
            return report

        # L3: Agent 进程 — Claude Code 是否存活
        report.l3_agent = status.get("active_processes", 0) >= 0
        report.l3_details = status.get("processes", [])

        return report
```

### 2.11 优雅缩容 Drain (T-025)

```python
# src/elastic_agent/core/scheduler/drain.py

class DrainManager:
    async def drain_node(self, node_id: str, policy: DrainPolicy) -> bool:
        """优雅缩容一个节点"""
        node = self.registry.get(node_id)

        # 1. 标记为 draining — 不再分配新任务
        self.registry.update_status(node_id, "draining")

        # 2. 通知 Harness
        if policy.notify_harness:
            await self.event_bus.emit(FrameworkEvent.NODE_DRAIN_START, {
                "node_id": node_id,
            })

        # 3. 等待当前任务完成
        runtime = self.get_runtime_client(node_id)
        try:
            await asyncio.wait_for(
                self._wait_tasks_complete(runtime),
                timeout=policy.timeout,
            )
        except asyncio.TimeoutError:
            logger.warning(f"Drain timeout for {node_id}, force terminating")

        # 4. 备份数据
        if policy.backup_before_terminate:
            await self._backup_workspace(node_id)

        # 5. 回收凭证
        cred_id = node.get("credential_id")
        if cred_id:
            self.credential_pool.release(cred_id)

        # 6. 终止实例
        await self.provider.terminate_instance(node_id)
        self.registry.remove(node_id)
        return True
```

### 2.12 ElasticAgentManager 主类 (T-016)

```python
# src/elastic_agent/manager/service.py

class ElasticAgentManager:
    """Elastic-Agent 框架入口 — 管理整个集群"""

    def __init__(
        self,
        provider: CloudProvider,
        credential_pool: CredentialPool | None = None,
        harness: Harness | None = None,
        config: ManagerConfig | None = None,
    ):
        self.provider = provider
        self.credentials = credential_pool
        self.harness = harness
        self.config = config or ManagerConfig()
        self.registry = NodeRegistry(self.config.registry_path)
        self.event_bus = EventBus()
        self.reconciler = CloudReconciler(provider, self.registry)
        self.health_checker = HealthChecker(provider)
        self.drain_manager = DrainManager(provider, self.registry, self.event_bus)
        self.external_api = ExternalAPIManager(self.event_bus, self.registry)

    async def start(self):
        """启动后台任务"""
        await self.reconciler.reconcile()  # 启动时对账
        asyncio.create_task(self._health_check_loop())
        asyncio.create_task(self._reconcile_loop())

    async def scale_out(self, count: int = 1,
                        instance_config: InstanceConfig | None = None) -> list[Instance]:
        """扩容 — 创建新 Worker 并初始化"""
        nodes = []
        for _ in range(count):
            config = instance_config or InstanceConfig(
                name=f"worker-{secrets.token_hex(4)}",
            )
            # 1. 创建实例
            instance = await self.provider.create_instance(config)
            await self.event_bus.emit(FrameworkEvent.NODE_CREATING, {"instance": instance})

            # 2. 等待运行
            instance = await self.provider.wait_until_running(instance.id)

            # 3. 选择凭证
            cred = None
            if self.credentials:
                cred = self.credentials.select(prefer_ip=instance.public_ip)

            # 4. 生成 Worker token
            worker_token = generate_worker_token()

            # 5. Bootstrap
            bootstrap = BootstrapPipeline(
                steps=self._get_bootstrap_steps(cred),
            )
            ctx = BootstrapContext(
                instance=instance,
                credential=cred,
                harness=self.harness,
                worker_token=worker_token,
                config=self.config,
            )
            success = await bootstrap.execute(ctx)
            if not success:
                await self.provider.terminate_instance(instance.id)
                if cred:
                    self.credentials.release(cred.id)
                continue

            # 6. 注册
            self.registry.add(instance, credential_id=cred.id if cred else None,
                              auth_token=worker_token)
            await self.event_bus.emit(FrameworkEvent.NODE_READY, {
                "node_id": instance.id,
                "private_ip": instance.private_ip,
                "public_ip": instance.public_ip,
            })
            nodes.append(instance)
        return nodes

    async def scale_in(self, count: int = 1) -> list[str]:
        """缩容 — 优雅地移除 Worker"""
        # 选择空闲的 Worker
        all_nodes = self.registry.list_all()
        idle_nodes = [n for n in all_nodes if n.get("status") == "idle"]
        victims = idle_nodes[:count]

        removed = []
        for node in victims:
            success = await self.drain_manager.drain_node(
                node["instance_id"],
                DrainPolicy(),
            )
            if success:
                removed.append(node["instance_id"])
        return removed

    async def list_nodes(self) -> list[dict]:
        return self.registry.list_all()

    async def get_cluster_status(self) -> dict:
        nodes = self.registry.list_all()
        return {
            "total": len(nodes),
            "running": sum(1 for n in nodes if n.get("status") == "running"),
            "draining": sum(1 for n in nodes if n.get("status") == "draining"),
            "provider": self.provider.__class__.__name__,
        }
```

---

## 3. 实现顺序与依赖关系

```
Week 1-2: 基础层
  T-001 项目脚手架
    ├── T-002 CloudProvider 抽象
    │   ├── T-003 阿里云 ECS Provider  ← 首选
    │   └── T-004 AWS EC2 Provider
    ├── T-005 Terraform 阿里云网络  ← 与 Provider 并行
    └── T-006 Terraform AWS 网络

Week 2-3: 通信层
  T-009 通信协议
    ├── T-007 Worker Runtime 服务
    ├── T-008 Worker Runtime 客户端
    └── T-010 认证

Week 3-4: 管理层
  T-011 节点注册表
  T-012 云端标签对账
  T-016 Manager FastAPI 服务
  T-017 Claude Code AgentType

Week 4-5: 外部 API + Bootstrap
  T-013 外部 API 轨迹流
  T-014 外部 API 文件传输
  T-015 外部 API 认证
  T-018 ~ T-023 Bootstrap Pipeline

Week 5-6: 稳定性 + UI
  T-024 健康检查
  T-025 Drain 机制
  T-026 凭证分发
  T-027 额度监控
  T-028 手动扩缩容 API
  T-029 基础 Web UI

Week 6-7: 测试
  T-100 ~ T-118 全部测试
```

### 关键依赖链

```
T-002 → T-003/T-004 → T-011 → T-012
                                  ↓
T-009 → T-007/T-008 → T-010 → T-016 → T-028
                         ↓
                      T-013/T-014 (外部 API 依赖 Worker Runtime 通道)
                         ↓
T-018 → T-019~T-022 → T-023 → T-025 (Drain 依赖 Bootstrap 完成)
```

---

## 4. 配置管理

### 4.1 Manager 配置文件

```yaml
# config.yaml — Manager 主配置

# 云服务商配置
provider:
  type: "aliyun"  # "aliyun" | "aws"

  aliyun:
    access_key_id: "${ALICLOUD_ACCESS_KEY_ID}"
    access_key_secret: "${ALICLOUD_ACCESS_KEY_SECRET}"
    region_id: "cn-hangzhou"
    image_id: "m-bp1xxxx"       # 自定义镜像
    instance_type: "ecs.c6.large"
    security_group_id: ""       # 从 terraform output 填入
    vswitch_id: ""              # 从 terraform output 填入
    key_pair_name: "elastic-agent-key"
    ssh_key_path: "~/.ssh/elastic-agent.pem"
    max_instances: 30

  aws:
    region: "ap-northeast-1"
    ami_id: "ami-xxxxx"
    default_instance_type: "t3.large"
    security_group_ids: []      # 从 terraform output 填入
    subnet_id: ""               # 从 terraform output 填入
    key_pair_name: "elastic-agent-key"
    ssh_key_path: "~/.ssh/elastic-agent.pem"
    max_instances: 30

# Worker 配置
worker:
  ssh_user: "root"              # 阿里云默认 root，AWS 默认 ubuntu
  runtime_port: 8080

# 凭证池
credentials:
  pool_file: "credentials.json"
  quota_threshold: 0.85

# 外部服务 API
external_api:
  enabled: true
  api_keys:
    - "${EXTERNAL_API_KEY}"
  trace_buffer_size: 10000      # 内存中缓存的轨迹条数
  trace_persist: false          # MVP 不持久化，后续加数据库

# 监控
monitor:
  health_check_interval: 30     # 秒
  reconcile_interval: 300       # 秒
  quota_check_interval: 60      # 秒

# 注册表
registry:
  path: "~/.elastic-agent/registry.json"
```

### 4.2 Terraform 输出集成

Manager 配置中的 `security_group_id`、`vswitch_id` 等字段从 Terraform 输出获取：

```bash
# 部署流程
cd infra/environments/aliyun-cn-hangzhou
terraform init
terraform apply

# 提取输出写入配置
SECURITY_GROUP_ID=$(terraform output -raw security_group_id)
VSWITCH_ID=$(terraform output -raw vswitch_ids | jq -r '.[0]')
KEY_PAIR_NAME=$(terraform output -raw key_pair_name)

# 更新 config.yaml 或通过环境变量传入
```

---

## 5. 测试策略

### 5.1 单元测试

使用 `pytest` + `pytest-asyncio`。所有云 SDK 调用通过 mock 隔离。

```python
# tests/unit/test_providers.py

@pytest.mark.asyncio
async def test_aliyun_create_instance():
    """验证阿里云 Provider 正确调用 SDK 并转换响应"""
    mock_client = Mock()
    mock_client.run_instances.return_value = Mock(
        body=Mock(instance_id_sets=Mock(
            instance_id_set=["i-bp1xxxx"]
        ))
    )
    provider = AliyunEcsProvider.__new__(AliyunEcsProvider)
    provider.client = mock_client
    provider.config = AliyunEcsConfig(...)

    instance = await provider.create_instance(InstanceConfig(name="test"))
    assert instance.id == "i-bp1xxxx"
    assert instance.provider == "aliyun"

@pytest.mark.asyncio
async def test_aws_create_instance():
    """验证 AWS Provider 正确调用 boto3"""
    # 类似结构
    ...
```

```python
# tests/unit/test_external_traces.py

@pytest.mark.asyncio
async def test_trace_stream_filters_by_node():
    """验证轨迹流只推送指定 Worker 的事件"""
    event_bus = EventBus()
    # 发送两个不同 Worker 的事件
    await event_bus.emit("log", {"worker_id": "w1", "data": "hello"})
    await event_bus.emit("log", {"worker_id": "w2", "data": "world"})
    # 订阅 w1 只收到 w1 的事件
    events = [e async for e in event_bus.subscribe(node_id="w1", limit=1)]
    assert len(events) == 1
    assert events[0].payload["worker_id"] == "w1"
```

### 5.2 集成测试

需要真实云资源。通过环境变量 `ELASTIC_AGENT_TEST_PROVIDER=aliyun` 控制。

```python
# tests/integration/test_aliyun_e2e.py

@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("ELASTIC_AGENT_TEST_PROVIDER") != "aliyun",
    reason="需要阿里云环境变量"
)
@pytest.mark.asyncio
async def test_aliyun_full_lifecycle():
    """阿里云 ECS 实例完整生命周期测试"""
    provider = AliyunEcsProvider(AliyunEcsConfig(
        access_key_id=os.environ["ALICLOUD_ACCESS_KEY_ID"],
        access_key_secret=os.environ["ALICLOUD_ACCESS_KEY_SECRET"],
        ...
    ))

    # 创建
    instance = await provider.create_instance(InstanceConfig(
        name="test-lifecycle",
        tags={"Test": "true"},
    ))
    assert instance.status == InstanceStatus.PENDING

    # 等待运行
    instance = await provider.wait_until_running(instance.id, timeout=120)
    assert instance.status == InstanceStatus.RUNNING
    assert instance.public_ip is not None

    # 停止
    await provider.stop_instance(instance.id)
    # ... wait

    # 释放
    await provider.terminate_instance(instance.id)
```

### 5.3 DryRun 测试

验证流程逻辑不消耗云资源。

```python
# tests/unit/test_dry_run.py

@pytest.mark.asyncio
async def test_scale_out_dry_run():
    """DryRunProvider 验证扩容流程"""
    provider = DryRunProvider()
    manager = ElasticAgentManager(provider=provider)
    nodes = await manager.scale_out(count=3)
    assert len(nodes) == 3
    assert len(provider.operations) == 3
    assert all(op[0] == "create" for op in provider.operations)
```

### 5.4 Terraform 测试

```bash
# 验证 Terraform 配置语法和计划
cd infra/environments/aliyun-cn-hangzhou
terraform init
terraform validate
terraform plan -out=tfplan

# 集成测试（创建真实资源后销毁）
terraform apply -auto-approve
terraform output  # 验证输出
terraform destroy -auto-approve
```

---

## 6. 与 Harness 的集成点

MVP 完成后，Harness（如 CCM、agent-ml-research）通过以下方式接入：

```python
# Harness 接入示例

from elastic_agent import ElasticAgentManager, AliyunEcsProvider

# 1. 选择 Provider（阿里云优先）
provider = AliyunEcsProvider(config)
# 或 AWSEc2Provider(config) — 接口一致

# 2. 初始化 Manager
manager = ElasticAgentManager(
    provider=provider,
    harness=MyHarness(app_config),
)

# 3. 启动服务
await manager.start()

# 4. 扩缩容
await manager.scale_out(count=2)
await manager.scale_in(count=1)

# 5. 外部服务消费数据
# WebSocket: ws://manager:8000/api/external/traces/{node_id}/stream?api_key=xxx
# REST: GET /api/external/traces/{node_id}?api_key=xxx
# 文件: GET /api/external/files/{node_id}/workspace/output.log?api_key=xxx
```

---

## 附录：技术选型确认

| 技术 | 选择 | 理由 |
|------|------|------|
| 语言 | Python 3.11+ | 与两个 Harness 统一，async 生态成熟 |
| 后端框架 | FastAPI | 原生 async + WebSocket + OpenAPI |
| 阿里云 SDK | alibabacloud_ecs20140526 | V2.0 新版，类型提示完整 |
| AWS SDK | boto3 | 标准选择 |
| IaC | Terraform | 唯一同时支持阿里云 + AWS 的成熟工具 |
| 前端 | React + Vite + Ant Design | 与 CCM 前端统一 |
| 测试 | pytest + pytest-asyncio | Python 异步测试标准 |
| 包管理 | uv | 快速依赖安装 |
| SSH | asyncssh | 原生 async SSH |
| WebSocket | websockets / FastAPI native | 标准选择 |
| 文件监听 | watchdog | 跨平台文件系统事件监听 |
| 配置 | Pydantic + YAML | 类型安全 + 可读性 |
