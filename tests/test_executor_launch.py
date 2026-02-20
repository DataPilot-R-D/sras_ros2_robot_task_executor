"""Launch integration tests for robot_task_executor_node."""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import unittest

try:
    import launch
    from launch.actions import ExecuteProcess
    import launch_ros.actions
    import launch_testing
    import launch_testing.actions
    import launch_testing.asserts
    import pytest
    import rclpy
    from rclpy.executors import SingleThreadedExecutor
    from std_msgs.msg import String

    ROS_TEST_DEPS_AVAILABLE = True
except Exception:
    ROS_TEST_DEPS_AVAILABLE = False


def _decode_json_message(raw: str) -> dict:
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


if not ROS_TEST_DEPS_AVAILABLE:

    class TestLaunchDepsUnavailable(unittest.TestCase):
        @unittest.skip("ROS2 launch_testing dependencies are not available in this environment")
        def test_launch_testing_dependencies_missing(self) -> None:
            pass

else:

    @pytest.mark.launch_test
    def generate_test_description() -> tuple[launch.LaunchDescription, dict]:
        test_dir = os.path.dirname(__file__)
        fake_nav2_server = ExecuteProcess(
            cmd=[
                sys.executable,
                os.path.join(test_dir, "helpers", "fake_nav2_action_server.py"),
                "--execute-delay-s",
                "1.5",
            ],
            name="fake_nav2_action_server",
            output="screen",
        )

        executor_node = launch_ros.actions.Node(
            package="sras_robot_task_executor",
            executable="robot_task_executor_node",
            name="robot_task_executor_node",
            output="screen",
            parameters=[
                {
                    "task_request_topic": "/reasoning/task_requests",
                    "task_status_topic": "/robot/task_status",
                    "alerts_topic": "/ui/alerts",
                    "executor_state_topic": "/executor_state",
                    "set_task_state_topic": "/ui/set_task_state",
                    "executor_tick_hz": 20.0,
                    "require_map": False,
                    "require_tf": False,
                    "require_nav_ready": True,
                    "action_server_wait_timeout_s": 1.0,
                    "goal_timeout_s": 10.0,
                    "use_json_transport_fallback": True,
                }
            ],
        )

        return (
            launch.LaunchDescription(
                [
                    fake_nav2_server,
                    executor_node,
                    launch_testing.actions.ReadyToTest(),
                ]
            ),
            {
                "executor_node": executor_node,
                "fake_nav2_server": fake_nav2_server,
            },
        )


    class TestExecutorLaunch(unittest.TestCase):
        @classmethod
        def setUpClass(cls) -> None:
            rclpy.init()

        @classmethod
        def tearDownClass(cls) -> None:
            if rclpy.ok():
                rclpy.shutdown()

        def setUp(self) -> None:
            self._lock = threading.Lock()
            self._status_msgs: list[dict] = []
            self._alert_msgs: list[dict] = []
            self._state_msgs: list[dict] = []

            self.node = rclpy.create_node("executor_launch_test_client")
            self._executor = SingleThreadedExecutor()
            self._executor.add_node(self.node)
            self._running = True
            self._spin_thread = threading.Thread(target=self._spin, daemon=True)
            self._spin_thread.start()

            self.status_sub = self.node.create_subscription(
                String,
                "/robot/task_status",
                self._on_status,
                50,
            )
            self.alert_sub = self.node.create_subscription(
                String,
                "/ui/alerts",
                self._on_alert,
                50,
            )
            self.state_sub = self.node.create_subscription(
                String,
                "/executor_state",
                self._on_state,
                50,
            )
            self.task_pub = self.node.create_publisher(String, "/reasoning/task_requests", 10)
            self.command_pub = self.node.create_publisher(String, "/ui/set_task_state", 10)

            # Allow publisher/subscriber graph to settle.
            time.sleep(0.3)

        def tearDown(self) -> None:
            self._running = False
            if self._spin_thread.is_alive():
                self._spin_thread.join(timeout=2.0)

            self._executor.remove_node(self.node)
            self.node.destroy_node()

        def _spin(self) -> None:
            while self._running and rclpy.ok():
                self._executor.spin_once(timeout_sec=0.1)

        def _on_status(self, msg: String) -> None:
            payload = _decode_json_message(msg.data)
            with self._lock:
                self._status_msgs.append(payload)

        def _on_alert(self, msg: String) -> None:
            payload = _decode_json_message(msg.data)
            with self._lock:
                self._alert_msgs.append(payload)

        def _on_state(self, msg: String) -> None:
            payload = _decode_json_message(msg.data)
            with self._lock:
                self._state_msgs.append(payload)

        def _publish_json(self, publisher: object, payload: dict) -> None:
            msg = String()
            msg.data = json.dumps(payload)
            publisher.publish(msg)

        def _wait_for(self, predicate: object, timeout_s: float, description: str) -> None:
            deadline = time.monotonic() + timeout_s
            while time.monotonic() < deadline:
                if predicate():
                    return
                time.sleep(0.05)
            self.fail(f"Timed out waiting for: {description}")

        def _task_states(self, task_id: str) -> list[str]:
            with self._lock:
                return [
                    str(item.get("state", ""))
                    for item in self._status_msgs
                    if str(item.get("task_id", "")) == task_id
                ]

        def _alert_states(self, task_id: str) -> list[str]:
            with self._lock:
                return [
                    str(item.get("state", ""))
                    for item in self._alert_msgs
                    if str(item.get("task_id", "")) == task_id
                ]

        def _latest_readiness_nav_ready(self) -> bool:
            with self._lock:
                if not self._state_msgs:
                    return False
                readiness = self._state_msgs[-1].get("readiness", {})
            return bool(readiness.get("nav_ready", False))

        def test_autonomous_execution_reaches_succeeded(self) -> None:
            self._wait_for(
                lambda: self._latest_readiness_nav_ready(),
                timeout_s=10.0,
                description="nav action readiness true in executor_state",
            )

            task_id = "autonomous-1"
            self._publish_json(
                self.task_pub,
                {
                    "task_id": task_id,
                    "task_type": "INSPECT_POI",
                    "goal": {"frame_id": "map", "x": 1.0, "y": 2.0, "yaw": 0.0},
                },
            )

            self._wait_for(
                lambda: "QUEUED" in self._task_states(task_id),
                timeout_s=5.0,
                description="QUEUED status",
            )
            self._wait_for(
                lambda: "DISPATCHED" in self._task_states(task_id),
                timeout_s=5.0,
                description="DISPATCHED status",
            )
            self._wait_for(
                lambda: "ACTIVE" in self._task_states(task_id),
                timeout_s=5.0,
                description="ACTIVE status",
            )
            self._wait_for(
                lambda: "SUCCEEDED" in self._task_states(task_id),
                timeout_s=8.0,
                description="SUCCEEDED status",
            )

        def test_hotl_pause_redefine_resume(self) -> None:
            self._wait_for(
                lambda: self._latest_readiness_nav_ready(),
                timeout_s=10.0,
                description="nav action readiness true in executor_state",
            )

            task_id = "hotl-1"
            self._publish_json(
                self.task_pub,
                {
                    "task_id": task_id,
                    "task_type": "INSPECT_POI",
                    "goal": {"frame_id": "map", "x": 0.0, "y": 0.0, "yaw": 0.0},
                },
            )
            self._wait_for(
                lambda: "ACTIVE" in self._task_states(task_id),
                timeout_s=5.0,
                description="ACTIVE before pause",
            )

            self._publish_json(
                self.command_pub,
                {
                    "task_id": task_id,
                    "command": "pause",
                },
            )
            self._wait_for(
                lambda: "PAUSED" in self._task_states(task_id),
                timeout_s=5.0,
                description="PAUSED status",
            )
            self._wait_for(
                lambda: "PAUSED" in self._alert_states(task_id),
                timeout_s=5.0,
                description="PAUSED alert",
            )

            self._publish_json(
                self.command_pub,
                {
                    "task_id": task_id,
                    "command": "redefine",
                    "task": {
                        "task_type": "PATROL_ROUTE",
                        "poses": [
                            {"frame_id": "map", "x": 0.0, "y": 0.0, "yaw": 0.0},
                            {"frame_id": "map", "x": 1.0, "y": 1.0, "yaw": 0.2},
                        ],
                    },
                },
            )
            self._wait_for(
                lambda: "REDEFINED" in self._task_states(task_id),
                timeout_s=5.0,
                description="REDEFINED status",
            )

            self._publish_json(
                self.command_pub,
                {
                    "task_id": task_id,
                    "command": "resume",
                },
            )
            self._wait_for(
                lambda: self._task_states(task_id).count("DISPATCHED") >= 2,
                timeout_s=6.0,
                description="second DISPATCHED after resume",
            )
            self._wait_for(
                lambda: "SUCCEEDED" in self._task_states(task_id),
                timeout_s=8.0,
                description="SUCCEEDED after resume",
            )


    @launch_testing.post_shutdown_test()
    class TestExecutorLaunchShutdown(unittest.TestCase):
        def test_exit_codes(self, proc_info: object) -> None:
            launch_testing.asserts.assertExitCodes(proc_info)
