import pytest

from sras_robot_task_executor.execution_core import (
    CommandRejectedError,
    StatusEvent,
    ValidatedTask,
)
from sras_robot_task_executor.multi_robot_execution_core import (
    MultiRobotExecutionCore,
)


def _make_task(
    task_id: str = "task-1",
    task_type: str = "PURSUE_THIEF",
    robot_id: str = "robot0",
) -> dict:
    return {
        "task_id": task_id,
        "task_type": task_type,
        "robot_id": robot_id,
        "goal": {"x": 1.0, "y": 2.0, "z": 0.0, "yaw": 0.0, "frame_id": "map"},
    }


def _ready_core() -> MultiRobotExecutionCore:
    core = MultiRobotExecutionCore(
        robot_ids=["robot0", "robot1"],
        require_map=False,
        require_tf=False,
        require_nav_ready=False,
    )
    return core


class TestEnqueueAndRouting:
    def test_routes_task_to_correct_robot_core(self) -> None:
        core = _ready_core()

        event0 = core.enqueue_task(_make_task("t0", "PURSUE_THIEF", "robot0"))
        event1 = core.enqueue_task(_make_task("t1", "BLOCK_EXIT", "robot1"))

        assert event0.state == "QUEUED"
        assert event0.robot_id == "robot0"
        assert event1.state == "QUEUED"
        assert event1.robot_id == "robot1"

    def test_unknown_robot_id_routes_to_fallback(self) -> None:
        core = _ready_core()

        event = core.enqueue_task(_make_task("t0", "INVESTIGATE_ALERT", "unknown"))
        assert event.state == "QUEUED"

    def test_missing_robot_id_routes_to_fallback(self) -> None:
        core = _ready_core()

        task = {
            "task_id": "t0",
            "task_type": "INVESTIGATE_ALERT",
            "goal": {"x": 1.0, "y": 2.0, "z": 0.0, "yaw": 0.0, "frame_id": "map"},
        }
        event = core.enqueue_task(task)
        assert event.state == "QUEUED"


class TestDispatchNextAll:
    def test_dispatches_from_all_cores(self) -> None:
        core = _ready_core()
        core.enqueue_task(_make_task("t0", "PURSUE_THIEF", "robot0"))
        core.enqueue_task(_make_task("t1", "BLOCK_EXIT", "robot1"))

        events = core.dispatch_next_all()

        assert len(events) == 2
        dispatched_ids = {e.task_id for e in events}
        assert dispatched_ids == {"t0", "t1"}
        for e in events:
            assert e.state == "DISPATCHED"

    def test_empty_cores_return_empty_list(self) -> None:
        core = _ready_core()
        events = core.dispatch_next_all()
        assert events == []

    def test_active_task_blocks_second_dispatch(self) -> None:
        core = _ready_core()
        core.enqueue_task(_make_task("t0", "PURSUE_THIEF", "robot0"))
        core.enqueue_task(_make_task("t1", "PURSUE_THIEF", "robot0"))

        first = core.dispatch_next_all()
        assert len(first) == 1
        assert first[0].task_id == "t0"

        second = core.dispatch_next_all()
        assert len(second) == 0


class TestPerRobotReadiness:
    def test_readiness_affects_specific_robot_only(self) -> None:
        core = MultiRobotExecutionCore(
            robot_ids=["robot0", "robot1"],
            require_map=True,
            require_tf=False,
            require_nav_ready=False,
        )
        core.update_readiness("robot0", map_ready=True)
        # robot1 map_ready defaults to True in core

        core.enqueue_task(_make_task("t0", "PURSUE_THIEF", "robot0"))
        core.enqueue_task(_make_task("t1", "BLOCK_EXIT", "robot1"))

        events = core.dispatch_next_all()
        assert len(events) == 2


class TestMarkActiveAndTerminal:
    def test_mark_active_for_robot(self) -> None:
        core = _ready_core()
        core.enqueue_task(_make_task("t0", "PURSUE_THIEF", "robot0"))
        core.dispatch_next_all()

        event = core.mark_active("robot0")
        assert event.state == "ACTIVE"
        assert event.robot_id == "robot0"

    def test_mark_terminal_for_robot(self) -> None:
        core = _ready_core()
        core.enqueue_task(_make_task("t0", "PURSUE_THIEF", "robot0"))
        core.dispatch_next_all()
        core.mark_active("robot0")

        event = core.mark_terminal("robot0", "SUCCEEDED", "Navigation complete")
        assert event.state == "SUCCEEDED"
        assert event.robot_id == "robot0"

    def test_mark_active_unknown_robot_raises(self) -> None:
        core = _ready_core()
        with pytest.raises(CommandRejectedError, match="Unknown robot_id"):
            core.mark_active("nonexistent")

    def test_mark_terminal_unknown_robot_raises(self) -> None:
        core = _ready_core()
        with pytest.raises(CommandRejectedError, match="Unknown robot_id"):
            core.mark_terminal("nonexistent", "FAILED", "error")


class TestHandleCommand:
    def test_command_routes_to_correct_robot(self) -> None:
        core = _ready_core()
        core.enqueue_task(_make_task("t0", "PURSUE_THIEF", "robot0"))
        core.dispatch_next_all()

        event = core.handle_command({
            "command": "cancel",
            "task_id": "t0",
            "robot_id": "robot0",
        })
        assert event.state == "CANCELED"

    def test_command_without_robot_id_searches_all(self) -> None:
        core = _ready_core()
        core.enqueue_task(_make_task("t0", "PURSUE_THIEF", "robot0"))
        core.dispatch_next_all()

        event = core.handle_command({
            "command": "cancel",
            "task_id": "t0",
        })
        assert event.state == "CANCELED"


class TestSnapshot:
    def test_snapshot_structure(self) -> None:
        core = _ready_core()
        core.enqueue_task(_make_task("t0", "PURSUE_THIEF", "robot0"))

        snap = core.snapshot()
        assert snap["robot_count"] == 2
        assert "robot0" in snap["per_robot"]
        assert "robot1" in snap["per_robot"]
        assert "fallback" in snap
        assert snap["per_robot"]["robot0"]["queue_size"] == 1
        assert snap["per_robot"]["robot1"]["queue_size"] == 0


class TestNewTaskTypes:
    def test_pursue_thief_accepted(self) -> None:
        core = _ready_core()
        event = core.enqueue_task(_make_task("t0", "PURSUE_THIEF", "robot0"))
        assert event.state == "QUEUED"

    def test_block_exit_accepted(self) -> None:
        core = _ready_core()
        event = core.enqueue_task(_make_task("t0", "BLOCK_EXIT", "robot1"))
        assert event.state == "QUEUED"

    def test_guard_asset_accepted(self) -> None:
        core = _ready_core()
        event = core.enqueue_task(_make_task("t0", "GUARD_ASSET", "robot1"))
        assert event.state == "QUEUED"


class TestGetActiveTask:
    def test_get_active_task_returns_task(self) -> None:
        core = _ready_core()
        core.enqueue_task(_make_task("t0", "PURSUE_THIEF", "robot0"))
        core.dispatch_next_all()

        active = core.get_active_task("robot0")
        assert active is not None
        assert active.task_id == "t0"

    def test_get_active_task_unknown_robot_returns_none(self) -> None:
        core = _ready_core()
        assert core.get_active_task("nonexistent") is None

    def test_get_active_task_no_active_returns_none(self) -> None:
        core = _ready_core()
        assert core.get_active_task("robot0") is None
