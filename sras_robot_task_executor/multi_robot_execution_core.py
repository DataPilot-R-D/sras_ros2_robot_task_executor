"""Multi-robot execution core using composition over per-robot TaskExecutionCore.

No ROS imports — fully unit-testable.
"""

from __future__ import annotations

from typing import Any

from .execution_core import (
    CommandRejectedError,
    QueueFullError,
    StatusEvent,
    TaskExecutionCore,
    TaskValidationError,
)


class MultiRobotExecutionCore:
    """Routes tasks to per-robot TaskExecutionCore instances by robot_id."""

    def __init__(
        self,
        robot_ids: list[str],
        *,
        max_queue_size: int = 100,
        allow_preemption: bool = False,
        require_map: bool = True,
        require_tf: bool = True,
        require_nav_ready: bool = True,
    ) -> None:
        self._cores: dict[str, TaskExecutionCore] = {}
        self._robot_ids = list(robot_ids)
        for rid in robot_ids:
            self._cores[rid] = TaskExecutionCore(
                max_queue_size=max_queue_size,
                allow_preemption=allow_preemption,
                require_map=require_map,
                require_tf=require_tf,
                require_nav_ready=require_nav_ready,
            )
        self._fallback_core = TaskExecutionCore(
            max_queue_size=max_queue_size,
            allow_preemption=allow_preemption,
            require_map=require_map,
            require_tf=require_tf,
            require_nav_ready=require_nav_ready,
        )

    @property
    def robot_ids(self) -> list[str]:
        return list(self._robot_ids)

    def get_core(self, robot_id: str) -> TaskExecutionCore | None:
        return self._cores.get(robot_id)

    def enqueue_task(self, raw_task: dict[str, Any]) -> StatusEvent:
        robot_id = raw_task.get("robot_id")
        robot_id_str = str(robot_id).strip() if robot_id is not None else ""

        core = self._cores.get(robot_id_str, self._fallback_core)
        return core.enqueue_task(raw_task)

    def dispatch_next_all(self) -> list[StatusEvent]:
        events: list[StatusEvent] = []
        for core in self._cores.values():
            event = core.dispatch_next()
            if event is not None:
                events.append(event)

        fallback_event = self._fallback_core.dispatch_next()
        if fallback_event is not None:
            events.append(fallback_event)

        return events

    def handle_command(self, payload: dict[str, Any]) -> StatusEvent:
        robot_id = payload.get("robot_id")
        robot_id_str = str(robot_id).strip() if robot_id is not None else ""

        if robot_id_str and robot_id_str in self._cores:
            return self._cores[robot_id_str].handle_command(payload)

        for core in self._cores.values():
            try:
                return core.handle_command(payload)
            except CommandRejectedError:
                continue

        return self._fallback_core.handle_command(payload)

    def update_readiness(
        self,
        robot_id: str,
        *,
        map_ready: bool | None = None,
        tf_ready: bool | None = None,
        nav_ready: bool | None = None,
    ) -> None:
        core = self._cores.get(robot_id)
        if core is not None:
            core.update_readiness(
                map_ready=map_ready,
                tf_ready=tf_ready,
                nav_ready=nav_ready,
            )

    def mark_active(self, robot_id: str) -> StatusEvent:
        core = self._cores.get(robot_id)
        if core is None:
            raise CommandRejectedError(f"Unknown robot_id: {robot_id}")
        return core.mark_active()

    def mark_terminal(
        self,
        robot_id: str,
        terminal_state: str,
        detail: str,
    ) -> StatusEvent:
        core = self._cores.get(robot_id)
        if core is None:
            raise CommandRejectedError(f"Unknown robot_id: {robot_id}")
        return core.mark_terminal(terminal_state, detail)

    def get_active_task(self, robot_id: str) -> Any:
        core = self._cores.get(robot_id)
        if core is None:
            return None
        return core.active_task

    def snapshot(self) -> dict[str, Any]:
        result: dict[str, Any] = {"robot_count": len(self._cores)}
        per_robot: dict[str, Any] = {}
        for rid, core in self._cores.items():
            per_robot[rid] = core.snapshot()
        result["per_robot"] = per_robot
        result["fallback"] = self._fallback_core.snapshot()
        return result
