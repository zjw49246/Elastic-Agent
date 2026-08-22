"""TaskSyncMapper — Worker-side dynamic sync mapping management.

T-034: Receives REGISTER_SYNC_MAPPING / UNREGISTER_SYNC_MAPPING from Manager,
maintains active per-task mappings, and bridges to FileSyncManager.

The TaskSyncMapper is the intermediary between protocol messages and
FileSyncManager. It translates wire-format messages into SyncMappingEntry
objects and handles the lifecycle of mappings (register → watch → unregister).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ActiveMapping:
    task_id: str
    book_slug: str
    oss_prefix: str
    watch_paths: list[str]
    session_path_hash: str = ""
    registered_at: datetime = field(default_factory=_utcnow)


class TaskSyncMapper:
    """Manages dynamic per-task sync mappings on the Worker side.

    Responsibilities:
      - Accept REGISTER_SYNC_MAPPING → create mapping + start watching
      - Accept UNREGISTER_SYNC_MAPPING → final sync + remove mapping
      - Path matching: given a file path, find which task it belongs to
      - Expose active mappings for introspection
    """

    def __init__(
        self,
        on_register: Callable[[ActiveMapping], Any] | None = None,
        on_unregister: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self._mappings: dict[str, ActiveMapping] = {}
        self._on_register = on_register
        self._on_unregister = on_unregister

    @property
    def active_mappings(self) -> dict[str, ActiveMapping]:
        return dict(self._mappings)

    @property
    def task_count(self) -> int:
        return len(self._mappings)

    def has_task(self, task_id: str) -> bool:
        return task_id in self._mappings

    def get_mapping(self, task_id: str) -> ActiveMapping | None:
        return self._mappings.get(task_id)

    def register(
        self,
        task_id: str,
        book_slug: str,
        oss_prefix: str,
        watch_paths: list[str],
        session_path_hash: str = "",
    ) -> ActiveMapping:
        """Register a new sync mapping for a task.

        If a mapping already exists for the task_id, it is replaced.
        """
        if task_id in self._mappings:
            logger.warning("Replacing existing mapping for task %s", task_id)

        mapping = ActiveMapping(
            task_id=task_id,
            book_slug=book_slug,
            oss_prefix=oss_prefix,
            watch_paths=watch_paths,
            session_path_hash=session_path_hash,
        )
        self._mappings[task_id] = mapping

        if self._on_register:
            self._on_register(mapping)

        logger.info(
            "Registered sync mapping: task=%s book=%s prefix=%s paths=%s",
            task_id, book_slug, oss_prefix, watch_paths,
        )
        return mapping

    async def unregister(self, task_id: str) -> bool:
        """Unregister a sync mapping. Triggers final sync via callback."""
        if task_id not in self._mappings:
            logger.warning("No mapping found for task %s to unregister", task_id)
            return False

        if self._on_unregister:
            await self._on_unregister(task_id)

        self._mappings.pop(task_id, None)
        logger.info("Unregistered sync mapping for task %s", task_id)
        return True

    def match_path(self, file_path: str) -> tuple[str, ActiveMapping] | None:
        """Match a file path to its owning task.

        Returns (task_id, mapping) or None if no match.
        """
        for task_id, mapping in self._mappings.items():
            for wp in mapping.watch_paths:
                wp_norm = wp.rstrip("/")
                if file_path.startswith(wp_norm + "/") or file_path == wp_norm:
                    return (task_id, mapping)
        return None

    def all_watch_paths(self) -> list[str]:
        """Return all watch paths across all active mappings."""
        paths: list[str] = []
        for mapping in self._mappings.values():
            paths.extend(mapping.watch_paths)
        return paths

    async def unregister_all(self) -> int:
        """Unregister all mappings. Returns count of mappings removed."""
        task_ids = list(self._mappings.keys())
        count = 0
        for task_id in task_ids:
            if await self.unregister(task_id):
                count += 1
        return count
