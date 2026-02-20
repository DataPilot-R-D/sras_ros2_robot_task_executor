"""Deterministic task execution core, independent from ROS transport."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import time
from typing import Any
from uuid import uuid4


TASK_TYPE_TO_NAV_ACTION = {
    "INSPECT_POI": "navigate_to_pose",
    "INSPECT_BLINDSPOT": "navigate_to_pose",
    "PATROL_ROUTE": "navigate_through_poses",
}

TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELED"}


class TaskValidationError(ValueError):
    """Raised when a task payload violates the executor contract."""


class QueueFullError(RuntimeError):
    """Raised when queue length reaches configured max size."""


class CommandRejectedError(RuntimeError):
    """Raised when a HOTL command is invalid for the current lifecycle state."""


@dataclass(frozen=True)
class ValidatedTask:
    """Normalized task payload accepted by the deterministic core."""

    task_id: str
    task_type: str
    nav_action: str
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class StatusEvent:
    """Task status event consumed by ROS publisher layer."""

    task_id: str
    state: str
    detail: str
    progress: float = 0.0
    nav_action: str | None = None


class TaskExecutionCore:
    """Queue, lifecycle, and command state machine for robot tasks."""

    def __init__(
        self,
        max_queue_size: int = 100,
        allow_preemption: bool = False,
        require_map: bool = True,
        require_tf: bool = True,
        require_nav_ready: bool = True,
    ) -> None:
        self.max_queue_size = max(1, int(max_queue_size))
        self.allow_preemption = bool(allow_preemption)
        self.require_map = bool(require_map)
        self.require_tf = bool(require_tf)
        self.require_nav_ready = bool(require_nav_ready)
        self._queue: deque[ValidatedTask] = deque()
        self._active_task: ValidatedTask | None = None
        self._paused_task: ValidatedTask | None = None
        self._map_ready = True
        self._tf_ready = True
        self._nav_ready = True
        self._last_block_signature: tuple[str, str] | None = None
        self._stats: dict[str, Any] = {
            "tasks_received": 0,
            "tasks_queued": 0,
            "tasks_dispatched": 0,
            "tasks_succeeded": 0,
            "tasks_failed": 0,
            "tasks_canceled": 0,
            "tasks_paused": 0,
            "tasks_resumed": 0,
            "tasks_redefined": 0,
            "tasks_invalid": 0,
            "queue_rejected": 0,
            "started_at_s": time.time(),
        }

    @property
    def active_task_id(self) -> str | None:
        return self._active_task.task_id if self._active_task else None

    @property
    def active_task(self) -> ValidatedTask | None:
        return self._active_task

    def update_readiness(
        self,
        map_ready: bool | None = None,
        tf_ready: bool | None = None,
        nav_ready: bool | None = None,
    ) -> None:
        if map_ready is not None:
            self._map_ready = bool(map_ready)
        if tf_ready is not None:
            self._tf_ready = bool(tf_ready)
        if nav_ready is not None:
            self._nav_ready = bool(nav_ready)

    def enqueue_task(self, raw_task: dict[str, Any]) -> StatusEvent:
        self._stats["tasks_received"] += 1
        task = self._validate_task(raw_task)

        if len(self._queue) >= self.max_queue_size:
            self._stats["queue_rejected"] += 1
            raise QueueFullError(f"Queue is full (max_queue_size={self.max_queue_size})")

        self._queue.append(task)
        self._stats["tasks_queued"] += 1
        return StatusEvent(
            task_id=task.task_id,
            state="QUEUED",
            detail="Task accepted and queued for deterministic Nav2 execution",
            progress=0.0,
        )

    def dispatch_next(self) -> StatusEvent | None:
        if self._active_task is not None:
            return None
        if self._paused_task is not None:
            return None
        if not self._queue:
            self._last_block_signature = None
            return None
        block_reason = self._dispatch_block_reason()
        if block_reason:
            task_id = self._queue[0].task_id
            signature = (task_id, block_reason)
            if self._last_block_signature == signature:
                return None
            self._last_block_signature = signature
            return StatusEvent(
                task_id=task_id,
                state="BLOCKED",
                detail=f"Dispatch blocked: {block_reason}",
                progress=0.0,
            )

        task = self._queue.popleft()
        self._last_block_signature = None
        self._active_task = task
        self._stats["tasks_dispatched"] += 1
        return StatusEvent(
            task_id=task.task_id,
            state="DISPATCHED",
            detail=f"Task dispatched via {task.nav_action}",
            progress=0.0,
            nav_action=task.nav_action,
        )

    def mark_active(self) -> StatusEvent:
        if self._active_task is None:
            raise CommandRejectedError("No active task to mark ACTIVE")

        return StatusEvent(
            task_id=self._active_task.task_id,
            state="ACTIVE",
            detail="Task is now active on Nav2 action server",
            progress=0.0,
            nav_action=self._active_task.nav_action,
        )

    def mark_terminal(self, terminal_state: str, detail: str) -> StatusEvent:
        state = str(terminal_state).upper()
        if state not in TERMINAL_STATES:
            raise CommandRejectedError(f"Unsupported terminal state: {terminal_state}")
        if self._active_task is None:
            raise CommandRejectedError("No active task to complete")

        task = self._active_task
        self._active_task = None
        if state == "SUCCEEDED":
            self._stats["tasks_succeeded"] += 1
        elif state == "FAILED":
            self._stats["tasks_failed"] += 1
        elif state == "CANCELED":
            self._stats["tasks_canceled"] += 1

        return StatusEvent(
            task_id=task.task_id,
            state=state,
            detail=detail,
            progress=1.0 if state == "SUCCEEDED" else 0.0,
            nav_action=task.nav_action,
        )

    def handle_command(self, command_payload: dict[str, Any]) -> StatusEvent:
        command = str(command_payload.get("command", "")).strip().lower()
        task_id = str(command_payload.get("task_id", "")).strip()
        if not command:
            raise CommandRejectedError("Missing command in payload")
        if command == "stop":
            command = "cancel"

        if command == "approve":
            task = self._find_task(task_id)
            if task is None:
                raise CommandRejectedError("Task not found for approve")
            return StatusEvent(
                task_id=task.task_id,
                state="INFO",
                detail="Approval is optional in Human-over-the-Loop mode",
                progress=0.0,
                nav_action=task.nav_action,
            )

        if command == "cancel":
            return self._cancel_task(task_id)

        if command == "pause":
            return self._pause_task(task_id)

        if command == "resume":
            return self._resume_task(task_id)

        if command == "redefine":
            return self._redefine_task(task_id, command_payload.get("task"))

        raise CommandRejectedError(f"Unsupported command: {command}")

    def snapshot(self) -> dict[str, Any]:
        snapshot = dict(self._stats)
        snapshot["queue_size"] = len(self._queue)
        snapshot["active_task_id"] = self.active_task_id
        snapshot["paused_task_id"] = self._paused_task.task_id if self._paused_task else None
        snapshot["readiness"] = {
            "map_ready": self._map_ready,
            "tf_ready": self._tf_ready,
            "nav_ready": self._nav_ready,
            "require_map": self.require_map,
            "require_tf": self.require_tf,
            "require_nav_ready": self.require_nav_ready,
            "block_reason": self._dispatch_block_reason(),
        }
        return snapshot

    def _validate_task(self, raw_task: dict[str, Any]) -> ValidatedTask:
        if not isinstance(raw_task, dict):
            self._stats["tasks_invalid"] += 1
            raise TaskValidationError("Task payload must be an object")

        task_type = str(raw_task.get("task_type", "")).strip().upper()
        if task_type not in TASK_TYPE_TO_NAV_ACTION:
            self._stats["tasks_invalid"] += 1
            raise TaskValidationError(f"Unsupported task_type: {task_type or '<empty>'}")

        task_id = str(raw_task.get("task_id", "")).strip() or f"task-{uuid4().hex[:12]}"
        nav_action = TASK_TYPE_TO_NAV_ACTION[task_type]

        if nav_action == "navigate_to_pose":
            goal = raw_task.get("goal")
            if not isinstance(goal, dict):
                self._stats["tasks_invalid"] += 1
                raise TaskValidationError("navigate_to_pose requires object field: goal")
        elif nav_action == "navigate_through_poses":
            poses = raw_task.get("poses")
            if not isinstance(poses, list) or not poses:
                self._stats["tasks_invalid"] += 1
                raise TaskValidationError("navigate_through_poses requires non-empty list: poses")

        return ValidatedTask(
            task_id=task_id,
            task_type=task_type,
            nav_action=nav_action,
            raw_payload=raw_task,
        )

    def _find_task(self, task_id: str) -> ValidatedTask | None:
        if self._active_task and (not task_id or self._active_task.task_id == task_id):
            return self._active_task
        if self._paused_task and (not task_id or self._paused_task.task_id == task_id):
            return self._paused_task
        for task in self._queue:
            if not task_id or task.task_id == task_id:
                return task
        return None

    def _cancel_task(self, task_id: str) -> StatusEvent:
        if self._active_task and (not task_id or self._active_task.task_id == task_id):
            return self.mark_terminal("CANCELED", "Task canceled by operator command")

        if self._paused_task and (not task_id or self._paused_task.task_id == task_id):
            paused = self._paused_task
            self._paused_task = None
            self._stats["tasks_canceled"] += 1
            return StatusEvent(
                task_id=paused.task_id,
                state="CANCELED",
                detail="Paused task canceled by operator command",
                progress=0.0,
                nav_action=paused.nav_action,
            )

        for index, task in enumerate(self._queue):
            if task_id and task.task_id != task_id:
                continue
            del self._queue[index]
            self._stats["tasks_canceled"] += 1
            return StatusEvent(
                task_id=task.task_id,
                state="CANCELED",
                detail="Queued task canceled by operator command",
                progress=0.0,
                nav_action=task.nav_action,
            )

        raise CommandRejectedError("Task not found for cancel")

    def _pause_task(self, task_id: str) -> StatusEvent:
        if self._active_task is None:
            raise CommandRejectedError("Cannot pause without an active task")
        if task_id and self._active_task.task_id != task_id:
            raise CommandRejectedError("Cannot pause a non-active task")

        task = self._active_task
        self._active_task = None
        self._paused_task = task
        self._stats["tasks_paused"] += 1
        return StatusEvent(
            task_id=task.task_id,
            state="PAUSED",
            detail="Active task canceled for pause; ready for resumable re-dispatch",
            progress=0.0,
            nav_action=task.nav_action,
        )

    def _resume_task(self, task_id: str) -> StatusEvent:
        if self._paused_task is None:
            raise CommandRejectedError("No paused task to resume")
        if task_id and self._paused_task.task_id != task_id:
            raise CommandRejectedError("Paused task id does not match resume command")
        if self._active_task is not None:
            raise CommandRejectedError("Cannot resume while another task is active")

        task = self._paused_task
        self._paused_task = None
        self._active_task = task
        self._stats["tasks_resumed"] += 1
        self._stats["tasks_dispatched"] += 1
        return StatusEvent(
            task_id=task.task_id,
            state="DISPATCHED",
            detail="Paused task resumed via deterministic re-dispatch",
            progress=0.0,
            nav_action=task.nav_action,
        )

    def _redefine_task(self, task_id: str, replacement_task: Any) -> StatusEvent:
        if not isinstance(replacement_task, dict):
            raise CommandRejectedError("redefine requires object field: task")

        target_id = task_id or str(replacement_task.get("task_id", "")).strip()
        if not target_id:
            raise CommandRejectedError("redefine requires task_id")

        normalized_payload = dict(replacement_task)
        normalized_payload["task_id"] = target_id
        replacement_validated = self._validate_task(normalized_payload)

        if self._active_task and self._active_task.task_id == target_id:
            raise CommandRejectedError("Cannot redefine active task; pause or cancel first")

        if self._paused_task and self._paused_task.task_id == target_id:
            self._paused_task = replacement_validated
            self._stats["tasks_redefined"] += 1
            return StatusEvent(
                task_id=target_id,
                state="REDEFINED",
                detail="Paused task redefined by operator",
                progress=0.0,
                nav_action=replacement_validated.nav_action,
            )

        for idx, queued_task in enumerate(self._queue):
            if queued_task.task_id != target_id:
                continue
            self._queue[idx] = replacement_validated
            self._stats["tasks_redefined"] += 1
            return StatusEvent(
                task_id=target_id,
                state="REDEFINED",
                detail="Queued task redefined by operator",
                progress=0.0,
                nav_action=replacement_validated.nav_action,
            )

        raise CommandRejectedError("Task not found for redefine")

    def _dispatch_block_reason(self) -> str | None:
        if self.require_map and not self._map_ready:
            return "map not ready"
        if self.require_tf and not self._tf_ready:
            return "tf not ready"
        if self.require_nav_ready and not self._nav_ready:
            return "nav action servers not ready"
        return None
