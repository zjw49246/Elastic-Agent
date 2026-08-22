from elastic_agent.core.providers.base import CloudProvider, Instance, InstanceConfig, InstanceState

__all__ = ["CloudProvider", "Instance", "InstanceConfig", "InstanceState"]


def create_provider(provider_type: str, config) -> CloudProvider:
    """Factory function to create a CloudProvider by type string."""
    if provider_type == "aliyun":
        from elastic_agent.core.providers.aliyun import AliyunProvider

        return AliyunProvider(config)
    elif provider_type == "aws":
        from elastic_agent.core.providers.aws import AWSProvider

        return AWSProvider(config)
    elif provider_type == "dryrun":
        from elastic_agent.testing import DryRunProvider

        return DryRunProvider()
    raise ValueError(f"Unknown provider type: {provider_type}")
