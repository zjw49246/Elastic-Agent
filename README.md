# Elastic-Agent

Elastic computing framework for managing cloud-based agent workers.

## Installation

```bash
uv add git+https://github.com/zjw49246/Elastic-Agent.git
```

## Overview

Elastic-Agent is a Python library that provides:

- **Multi-cloud resource management** — Unified CloudProvider interface for Alibaba Cloud ECS and AWS EC2
- **Worker Runtime** — WebSocket-based communication between Manager and Workers
- **Task scheduling** — Capacity-aware task distribution with pluggable Harness interface
- **File sync** — Automatic file synchronization from Workers to OSS/S3
- **Credential management** — Account pool with auto-login, quota monitoring, and rotation

## Usage

```python
from elastic_agent.manager import ElasticAgentManager
from elastic_agent.core.providers import AliyunProvider
from elastic_agent.harness import Harness

class MyHarness(Harness):
    def get_bootstrap_steps(self):
        return [...]

manager = ElasticAgentManager(
    harness=MyHarness(),
    provider=AliyunProvider(config),
)
app = manager.create_app()
```

## Development

```bash
uv sync --extra dev
pytest
```
