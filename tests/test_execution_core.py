import unittest

from sras_robot_task_executor.execution_core import (
    CommandRejectedError,
    QueueFullError,
    TaskExecutionCore,
    TaskValidationError,
)


def _inspect_task(task_id: str) -> dict:
    return {
        "task_id": task_id,
        "task_type": "INSPECT_POI",
        "goal": {"frame_id": "map", "x": 1.0, "y": 2.0, "yaw": 0.0},
    }


def _patrol_task(task_id: str) -> dict:
    return {
        "task_id": task_id,
        "task_type": "PATROL_ROUTE",
        "poses": [
            {"frame_id": "map", "x": 0.0, "y": 0.0, "yaw": 0.0},
            {"frame_id": "map", "x": 1.0, "y": 1.0, "yaw": 0.5},
        ],
    }


def _report_task(task_id: str) -> dict:
    return {
        "task_id": task_id,
        "task_type": "REPORT",
        "incident_key": "incident-1",
        "report_data": {"summary": "all clear"},
    }


class TaskExecutionCoreTests(unittest.TestCase):
    def test_rejects_unknown_task_type(self) -> None:
        core = TaskExecutionCore(max_queue_size=2, allow_preemption=False)
        with self.assertRaises(TaskValidationError):
            core.enqueue_task({"task_id": "t-1", "task_type": "UNKNOWN"})

    def test_dispatch_maps_task_types_to_nav_actions(self) -> None:
        core = TaskExecutionCore(max_queue_size=2, allow_preemption=False)
        core.enqueue_task(_inspect_task("inspect-1"))
        event = core.dispatch_next()
        self.assertEqual(event.task_id, "inspect-1")
        self.assertEqual(event.state, "DISPATCHED")
        self.assertEqual(event.nav_action, "navigate_to_pose")

        core.mark_terminal("SUCCEEDED", "done")
        core.enqueue_task(_patrol_task("patrol-1"))
        event = core.dispatch_next()
        self.assertEqual(event.task_id, "patrol-1")
        self.assertEqual(event.nav_action, "navigate_through_poses")

    def test_autonomous_mode_does_not_require_approve_before_dispatch(self) -> None:
        core = TaskExecutionCore(max_queue_size=2, allow_preemption=False)
        core.enqueue_task(_inspect_task("inspect-1"))
        dispatched = core.dispatch_next()
        self.assertIsNotNone(dispatched)
        self.assertEqual(dispatched.state, "DISPATCHED")

    def test_investigate_alert_maps_to_navigate_to_pose(self) -> None:
        core = TaskExecutionCore(max_queue_size=2, allow_preemption=False)
        core.enqueue_task(
            {
                "task_id": "investigate-1",
                "task_type": "INVESTIGATE_ALERT",
                "goal": {"frame_id": "map", "x": 1.0, "y": 1.0, "yaw": 0.0},
            }
        )
        event = core.dispatch_next()
        self.assertIsNotNone(event)
        self.assertEqual(event.task_id, "investigate-1")
        self.assertEqual(event.nav_action, "navigate_to_pose")

    def test_report_task_type_maps_to_publish_report(self) -> None:
        core = TaskExecutionCore(max_queue_size=2, allow_preemption=False)
        core.enqueue_task(_report_task("report-1"))
        event = core.dispatch_next()
        self.assertIsNotNone(event)
        self.assertEqual(event.task_id, "report-1")
        self.assertEqual(event.nav_action, "publish_report")

    def test_report_task_requires_report_data_field(self) -> None:
        core = TaskExecutionCore(max_queue_size=2, allow_preemption=False)
        with self.assertRaises(TaskValidationError):
            core.enqueue_task(
                {
                    "task_id": "report-1",
                    "task_type": "REPORT",
                }
            )

    def test_queue_is_bounded(self) -> None:
        core = TaskExecutionCore(max_queue_size=2, allow_preemption=False)
        core.enqueue_task(_inspect_task("t-1"))
        core.enqueue_task(_inspect_task("t-2"))
        with self.assertRaises(QueueFullError):
            core.enqueue_task(_inspect_task("t-3"))

    def test_pause_resume_flow_is_cancel_then_redispatch(self) -> None:
        core = TaskExecutionCore(max_queue_size=2, allow_preemption=False)
        core.enqueue_task(_inspect_task("t-1"))
        core.dispatch_next()

        paused = core.handle_command({"task_id": "t-1", "command": "pause"})
        self.assertEqual(paused.state, "PAUSED")
        self.assertIn("canceled", paused.detail.lower())
        self.assertIsNone(core.active_task_id)

        resumed = core.handle_command({"task_id": "t-1", "command": "resume"})
        self.assertEqual(resumed.state, "DISPATCHED")
        self.assertEqual(resumed.task_id, "t-1")
        self.assertEqual(core.active_task_id, "t-1")

    def test_cancel_supports_active_and_queued_tasks(self) -> None:
        core = TaskExecutionCore(max_queue_size=3, allow_preemption=False)
        core.enqueue_task(_inspect_task("active-1"))
        core.enqueue_task(_inspect_task("queued-1"))
        core.dispatch_next()

        canceled_queued = core.handle_command({"task_id": "queued-1", "command": "cancel"})
        self.assertEqual(canceled_queued.state, "CANCELED")
        self.assertEqual(canceled_queued.task_id, "queued-1")

        canceled_active = core.handle_command({"task_id": "active-1", "command": "cancel"})
        self.assertEqual(canceled_active.state, "CANCELED")
        self.assertEqual(canceled_active.task_id, "active-1")
        self.assertIsNone(core.active_task_id)

    def test_stop_alias_cancels_active_task(self) -> None:
        core = TaskExecutionCore(max_queue_size=2, allow_preemption=False)
        core.enqueue_task(_inspect_task("task-1"))
        core.dispatch_next()
        stopped = core.handle_command({"task_id": "task-1", "command": "stop"})
        self.assertEqual(stopped.state, "CANCELED")
        self.assertEqual(stopped.task_id, "task-1")

    def test_rejects_resume_when_not_paused(self) -> None:
        core = TaskExecutionCore(max_queue_size=2, allow_preemption=False)
        with self.assertRaises(CommandRejectedError):
            core.handle_command({"task_id": "t-1", "command": "resume"})

    def test_pause_blocks_dispatch_of_other_queued_tasks(self) -> None:
        core = TaskExecutionCore(max_queue_size=3, allow_preemption=False)
        core.enqueue_task(_inspect_task("t-1"))
        core.enqueue_task(_inspect_task("t-2"))
        core.dispatch_next()
        core.handle_command({"task_id": "t-1", "command": "pause"})
        self.assertIsNone(core.dispatch_next())

    def test_dispatch_blocked_when_required_map_not_ready(self) -> None:
        core = TaskExecutionCore(
            max_queue_size=2,
            allow_preemption=False,
            require_map=True,
            require_tf=False,
            require_nav_ready=False,
        )
        core.enqueue_task(_inspect_task("t-1"))
        core.update_readiness(map_ready=False)
        blocked = core.dispatch_next()
        self.assertIsNotNone(blocked)
        self.assertEqual(blocked.state, "BLOCKED")
        self.assertIn("map", blocked.detail.lower())

        core.update_readiness(map_ready=True)
        dispatched = core.dispatch_next()
        self.assertIsNotNone(dispatched)
        self.assertEqual(dispatched.state, "DISPATCHED")

    def test_non_required_readiness_signal_does_not_block(self) -> None:
        core = TaskExecutionCore(
            max_queue_size=2,
            allow_preemption=False,
            require_map=False,
            require_tf=False,
            require_nav_ready=False,
        )
        core.enqueue_task(_inspect_task("t-1"))
        core.update_readiness(map_ready=False, tf_ready=False, nav_ready=False)
        dispatched = core.dispatch_next()
        self.assertIsNotNone(dispatched)
        self.assertEqual(dispatched.state, "DISPATCHED")

    def test_blocked_event_is_debounced_until_readiness_changes(self) -> None:
        core = TaskExecutionCore(
            max_queue_size=2,
            allow_preemption=False,
            require_map=True,
            require_tf=False,
            require_nav_ready=False,
        )
        core.enqueue_task(_inspect_task("t-1"))
        core.update_readiness(map_ready=False)
        first_blocked = core.dispatch_next()
        self.assertIsNotNone(first_blocked)
        self.assertEqual(first_blocked.state, "BLOCKED")
        self.assertIsNone(core.dispatch_next())

        core.update_readiness(map_ready=True)
        dispatched = core.dispatch_next()
        self.assertIsNotNone(dispatched)
        self.assertEqual(dispatched.state, "DISPATCHED")

    def test_snapshot_exposes_block_reason(self) -> None:
        core = TaskExecutionCore(
            max_queue_size=2,
            allow_preemption=False,
            require_map=False,
            require_tf=False,
            require_nav_ready=True,
        )
        core.update_readiness(nav_ready=False)
        snapshot = core.snapshot()
        self.assertEqual(snapshot["readiness"]["block_reason"], "nav action servers not ready")

    def test_redefine_updates_queued_task(self) -> None:
        core = TaskExecutionCore(max_queue_size=2, allow_preemption=False)
        core.enqueue_task(_inspect_task("queued-1"))
        redefine_event = core.handle_command(
            {
                "task_id": "queued-1",
                "command": "redefine",
                "task": _patrol_task("queued-1"),
            }
        )
        self.assertEqual(redefine_event.state, "REDEFINED")
        dispatched = core.dispatch_next()
        self.assertIsNotNone(dispatched)
        self.assertEqual(dispatched.task_id, "queued-1")
        self.assertEqual(dispatched.nav_action, "navigate_through_poses")

    def test_redefine_updates_paused_task_then_resume_dispatches_redefined_task(self) -> None:
        core = TaskExecutionCore(max_queue_size=2, allow_preemption=False)
        core.enqueue_task(_inspect_task("task-1"))
        core.dispatch_next()
        core.handle_command({"task_id": "task-1", "command": "pause"})
        core.handle_command(
            {
                "task_id": "task-1",
                "command": "redefine",
                "task": _patrol_task("task-1"),
            }
        )
        resumed = core.handle_command({"task_id": "task-1", "command": "resume"})
        self.assertEqual(resumed.state, "DISPATCHED")
        self.assertEqual(resumed.nav_action, "navigate_through_poses")

    def test_redefine_rejects_active_task(self) -> None:
        core = TaskExecutionCore(max_queue_size=2, allow_preemption=False)
        core.enqueue_task(_inspect_task("task-1"))
        core.dispatch_next()
        with self.assertRaises(CommandRejectedError):
            core.handle_command(
                {
                    "task_id": "task-1",
                    "command": "redefine",
                    "task": _patrol_task("task-1"),
                }
            )


if __name__ == "__main__":
    unittest.main()
