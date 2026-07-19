"""BindingManager — wake/sleep lifecycle for account↔machine↔EIP bindings.

Turns the durable binding map (``AccountBindingStore``) into behaviour:

* **Lazy build** — the first time an account is acquired it gets a machine
  (create → wait running → allocate+associate EIP → provision → login) and the
  binding is persisted. Building is the only expensive path.
* **Wake** — a subsequent acquire just ``start_instance`` s the bound box and
  re-associates the EIP (idempotent belt-and-suspenders); no re-install, no
  re-login (auth.json rode the persistent EBS root).
* **Warm-hold then sleep** — ``release`` moves the box to WARM for
  ``warm_seconds``; ``reap`` later ``stop_instance`` s boxes whose warm grace
  elapsed. Dense back-to-back jobs re-acquire during WARM and skip the
  stop/start churn. The box is **stopped, never terminated**, so its IP and
  device stay put.

The environment-specific expensive steps (bootstrap/install, codex login) are
injected as async hooks so this module stays free of SSH/codex specifics and is
unit-testable against ``DryRunProvider``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from typing import Awaitable, Callable

from elastic_agent.core.account_binding import (
    AccountBinding,
    AccountBindingStore,
    BindingState,
)
from elastic_agent.core.providers.base import CloudProvider, InstanceConfig

logger = logging.getLogger(__name__)

# An async hook that acts on a freshly-built box (bootstrap/install, or login).
BindingHook = Callable[[AccountBinding], Awaitable[None]]


async def _noop(_binding: AccountBinding) -> None:
    return None


class BindingManager:
    def __init__(
        self,
        provider: CloudProvider,
        store: AccountBindingStore,
        *,
        warm_seconds: float = 300.0,
        provision_box: BindingHook | None = None,
        login_box: BindingHook | None = None,
        wait_timeout: int = 300,
    ) -> None:
        self._provider = provider
        self._store = store
        self._warm_seconds = warm_seconds
        self._provision_box = provision_box or _noop
        self._login_box = login_box or _noop
        self._wait_timeout = wait_timeout
        # Serialize all lifecycle ops per account so two jobs can't race on
        # the same box (double-build / start-while-stopping).
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def _lock(self, email: str) -> asyncio.Lock:
        return self._locks[email]

    # -- acquire (build or wake) ------------------------------------------

    async def acquire(
        self,
        email: str,
        *,
        account_id: str = "",
        instance_config: InstanceConfig | None = None,
        config_dir: str = "",
    ) -> AccountBinding:
        """Return a RUNNING, logged-in box for ``email``, building or waking.

        Idempotent for RUNNING/WARM bindings. On any failure the binding is
        marked ERROR (with the message) and the exception re-raised so the
        caller can decline / rotate to another account.
        """
        async with self._lock(email):
            binding = await self._store.get(email)
            if binding is None or binding.state in (
                BindingState.UNBOUND,
                BindingState.ERROR,
            ):
                if instance_config is None:
                    raise ValueError(
                        f"no machine for {email} yet and no instance_config to build one"
                    )
                return await self._build(email, account_id, instance_config, config_dir)

            if binding.state == BindingState.RUNNING:
                return binding
            if binding.state == BindingState.WARM:
                return await self._cancel_warm(binding)
            if binding.state in (BindingState.STOPPED, BindingState.PROVISIONING):
                return await self._wake(binding)
            raise ValueError(f"unexpected binding state {binding.state!r} for {email}")

    async def _build(
        self,
        email: str,
        account_id: str,
        instance_config: InstanceConfig,
        config_dir: str,
    ) -> AccountBinding:
        binding = AccountBinding(
            email=email,
            account_id=account_id,
            config_dir=config_dir,
            state=BindingState.PROVISIONING,
        )
        await self._store.upsert(binding)
        try:
            inst = await self._provider.create_instance(instance_config)
            binding.instance_id = inst.instance_id
            binding.region = inst.region
            await self._store.upsert(binding)

            await self._provider.wait_until_running(inst.instance_id, self._wait_timeout)

            eip = await self._provider.allocate_eip(
                tags={"account": email, "role": "codex-account-box"}
            )
            binding.eip_allocation_id = eip.allocation_id
            assoc = await self._provider.associate_eip(
                inst.instance_id, eip.allocation_id
            )
            binding.eip_ip = assoc.public_ip or eip.public_ip
            await self._store.upsert(binding)

            # Expensive, one-time: install runtime/deps, then codex login. Auth
            # lands on the persistent EBS root and never needs redoing.
            await self._provision_box(binding)
            await self._login_box(binding)

            binding.state = BindingState.RUNNING
            binding.last_used_at = time.time()
            binding.error = None
            await self._store.upsert(binding)
            logger.info("Built bound box for %s: %s @ %s",
                        email, binding.instance_id, binding.eip_ip)
            return binding
        except Exception as exc:  # noqa: BLE001
            binding.state = BindingState.ERROR
            binding.error = str(exc)
            await self._store.upsert(binding)
            logger.exception("Failed to build bound box for %s", email)
            raise

    async def _wake(self, binding: AccountBinding) -> AccountBinding:
        try:
            await self._provider.start_instance(binding.instance_id)
            await self._provider.wait_until_running(
                binding.instance_id, self._wait_timeout
            )
            # EIP stays associated across stop/start, but re-associate anyway
            # (idempotent) to be robust and to refresh the recorded IP.
            if binding.eip_allocation_id:
                assoc = await self._provider.associate_eip(
                    binding.instance_id, binding.eip_allocation_id
                )
                if assoc.public_ip:
                    binding.eip_ip = assoc.public_ip
            binding.state = BindingState.RUNNING
            binding.warm_until = None
            binding.last_used_at = time.time()
            binding.error = None
            await self._store.upsert(binding)
            logger.info("Woke bound box for %s: %s @ %s",
                        binding.email, binding.instance_id, binding.eip_ip)
            return binding
        except Exception as exc:  # noqa: BLE001
            binding.state = BindingState.ERROR
            binding.error = str(exc)
            await self._store.upsert(binding)
            logger.exception("Failed to wake bound box for %s", binding.email)
            raise

    async def _cancel_warm(self, binding: AccountBinding) -> AccountBinding:
        binding.state = BindingState.RUNNING
        binding.warm_until = None
        binding.last_used_at = time.time()
        await self._store.upsert(binding)
        return binding

    # -- release / sleep --------------------------------------------------

    async def release(self, email: str) -> AccountBinding | None:
        """Job done — enter warm-hold. ``reap`` stops it once the grace ends."""
        async with self._lock(email):
            binding = await self._store.get(email)
            if binding is None:
                return None
            binding.state = BindingState.WARM
            binding.warm_until = time.time() + self._warm_seconds
            binding.last_used_at = time.time()
            await self._store.upsert(binding)
            return binding

    async def sleep_now(self, email: str) -> AccountBinding | None:
        """Force-stop immediately, skipping the warm grace."""
        async with self._lock(email):
            binding = await self._store.get(email)
            if binding is None or binding.instance_id is None:
                return binding
            await self._provider.stop_instance(binding.instance_id)
            binding.state = BindingState.STOPPED
            binding.warm_until = None
            await self._store.upsert(binding)
            return binding

    async def reap(self, now: float | None = None) -> list[str]:
        """Stop every WARM box whose grace elapsed. Returns emails stopped.

        Call periodically. Skips (leaves WARM) any box that failed to stop so a
        transient provider error is retried next tick rather than lost.
        """
        now = now if now is not None else time.time()
        stopped: list[str] = []
        for binding in await self._store.by_state(BindingState.WARM):
            if binding.warm_until is not None and binding.warm_until > now:
                continue
            async with self._lock(binding.email):
                fresh = await self._store.get(binding.email)
                if fresh is None or fresh.state != BindingState.WARM:
                    continue  # re-acquired during the tick
                if fresh.warm_until is not None and fresh.warm_until > now:
                    continue
                if fresh.instance_id is None:
                    continue
                try:
                    await self._provider.stop_instance(fresh.instance_id)
                except Exception:  # noqa: BLE001
                    logger.exception("reap: stop failed for %s; will retry",
                                     fresh.email)
                    continue
                fresh.state = BindingState.STOPPED
                fresh.warm_until = None
                await self._store.upsert(fresh)
                stopped.append(fresh.email)
        if stopped:
            logger.info("reap: stopped %d warm box(es): %s", len(stopped), stopped)
        return stopped

    # -- decommission (retire an account entirely) ------------------------

    async def decommission(self, email: str) -> bool:
        """Terminate the box, release its EIP, drop the binding. Irreversible."""
        async with self._lock(email):
            binding = await self._store.get(email)
            if binding is None:
                return False
            if binding.instance_id is not None:
                try:
                    await self._provider.terminate_instance(binding.instance_id)
                except Exception:  # noqa: BLE001
                    logger.exception("decommission: terminate failed for %s", email)
            if binding.eip_allocation_id is not None:
                try:
                    await self._provider.disassociate_eip(binding.eip_allocation_id)
                except Exception:  # noqa: BLE001
                    pass
                try:
                    await self._provider.release_eip(binding.eip_allocation_id)
                except Exception:  # noqa: BLE001
                    logger.exception("decommission: release EIP failed for %s", email)
            return await self._store.remove(email)
