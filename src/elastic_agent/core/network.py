"""Network-path selection for Manager-initiated Worker administration."""

from __future__ import annotations

from typing import Any


def worker_management_host(
    *nodes: Any,
    provider_type: str | None = None,
) -> str | None:
    """Return the safest reachable address for Manager-to-Worker traffic.

    AWS security-group references identify ENIs on the private VPC path; they
    are not a reliable authorization boundary when the Manager connects to a
    Worker's public IPv4/EIP.  Prefer a private address for AWS, while retaining
    the historical public-first behavior for other providers.  Multiple node
    snapshots may be supplied in freshness order.
    """

    candidates = tuple(node for node in nodes if node is not None)
    if not candidates:
        return None

    kind = (provider_type or "").strip().casefold()
    if not kind:
        kind = next(
            (
                str(getattr(node, "platform", "") or "").strip().casefold()
                for node in candidates
                if getattr(node, "platform", None)
            ),
            "",
        )

    private_addresses = [
        getattr(node, "private_ip", None) for node in candidates
    ]
    public_addresses = [
        getattr(node, "public_ip", None) for node in candidates
    ]
    ordered = (
        private_addresses + public_addresses
        if kind == "aws"
        else public_addresses + private_addresses
    )
    return next((address for address in ordered if address), None)
