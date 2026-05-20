"""CLI entry point for starting a Worker Runtime process.

Used by LocalWorkerProcess (T-205) and can be invoked directly:

    python -m elastic_agent.worker.runtime_main \
        --manager-url ws://localhost:8000/ws/runtime \
        --token worker-token-1
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from elastic_agent.worker.runtime import WorkerRuntime


def main() -> None:
    parser = argparse.ArgumentParser(description="Elastic-Agent Worker Runtime")
    parser.add_argument("--manager-url", required=True, help="Manager WebSocket URL")
    parser.add_argument("--token", required=True, help="Worker auth token")
    parser.add_argument("--worker-id", default=None, help="Worker ID")
    parser.add_argument("--heartbeat-interval", type=int, default=30, help="Heartbeat interval (s)")
    parser.add_argument("--log-dir", default="logs", help="Log directory")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
    )

    runtime = WorkerRuntime(
        manager_url=args.manager_url,
        auth_token=args.token,
        worker_id=args.worker_id,
        heartbeat_interval=args.heartbeat_interval,
        log_dir=args.log_dir,
    )

    async def _run() -> None:
        try:
            await runtime.run()
        except KeyboardInterrupt:
            pass
        finally:
            await runtime.stop()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
