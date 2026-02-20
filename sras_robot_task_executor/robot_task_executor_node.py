#!/usr/bin/env python3
"""ROS2 scaffold node for Robot Task Executor."""

from __future__ import annotations

import json
import time
from typing import Any

import rclpy
from rclpy.node import Node
from std_msgs.msg import String
from std_srvs.srv import Trigger


class RobotTaskExecutorNode(Node):
    """Minimal executor scaffold.

    Note: this is intentionally lightweight. Nav2 action execution wiring
    is added in implementation phases from issue #44.
    """

    def __init__(self) -> None:
        super().__init__("robot_task_executor_node")
        self._declare_parameters()
        self._load_config()
        self._setup_ros()
        self._log_startup()

    def _declare_parameters(self) -> None:
        self.declare_parameter("task_request_topic", "/reasoning/task_requests")
        self.declare_parameter("task_status_topic", "/robot/task_status")
        self.declare_parameter("alerts_topic", "/ui/alerts")
        self.declare_parameter("executor_state_topic", "~/executor_state")
        self.declare_parameter("set_task_state_topic", "/ui/set_task_state")

        self.declare_parameter("nav_to_pose_action", "/navigate_to_pose")
        self.declare_parameter("nav_through_poses_action", "/navigate_through_poses")
        self.declare_parameter("action_server_wait_timeout_s", 5.0)
        self.declare_parameter("goal_timeout_s", 120.0)
        self.declare_parameter("cancel_timeout_s", 5.0)

        self.declare_parameter("executor_tick_hz", 2.0)
        self.declare_parameter("max_queue_size", 100)
        self.declare_parameter("max_active_tasks", 1)
        self.declare_parameter("allow_preemption", False)

        self.declare_parameter("require_map", True)
        self.declare_parameter("map_stale_timeout_s", 5.0)
        self.declare_parameter("require_tf", True)
        self.declare_parameter("tf_stale_timeout_s", 2.0)
        self.declare_parameter("require_nav_ready", True)

        self.declare_parameter("publish_executor_state", True)
        self.declare_parameter("journal_enabled", False)
        self.declare_parameter("journal_path", "data/executor_journal.db")
        self.declare_parameter("use_json_transport_fallback", True)

    def _load_config(self) -> None:
        self.task_request_topic = str(self.get_parameter("task_request_topic").value)
        self.task_status_topic = str(self.get_parameter("task_status_topic").value)
        self.alerts_topic = str(self.get_parameter("alerts_topic").value)
        self.executor_state_topic = str(self.get_parameter("executor_state_topic").value)
        self.set_task_state_topic = str(self.get_parameter("set_task_state_topic").value)

        self.nav_to_pose_action = str(self.get_parameter("nav_to_pose_action").value)
        self.nav_through_poses_action = str(self.get_parameter("nav_through_poses_action").value)

        self.tick_hz = max(0.1, float(self.get_parameter("executor_tick_hz").value))
        self.use_json_transport_fallback = bool(
            self.get_parameter("use_json_transport_fallback").value
        )

    def _setup_ros(self) -> None:
        self.task_sub = self.create_subscription(
            String,
            self.task_request_topic,
            self._task_request_callback,
            10,
        )
        self.status_pub = self.create_publisher(String, self.task_status_topic, 10)
        self.state_pub = self.create_publisher(String, self.executor_state_topic, 10)
        self.stats_srv = self.create_service(Trigger, "~/get_stats", self._get_stats_callback)
        self.tick_timer = self.create_timer(1.0 / self.tick_hz, self._tick_callback)

        self._stats: dict[str, Any] = {
            "tasks_received": 0,
            "tasks_dispatched": 0,
            "tasks_failed": 0,
            "tasks_canceled": 0,
            "started_at_s": time.time(),
        }

    def _log_startup(self) -> None:
        self.get_logger().info("=" * 60)
        self.get_logger().info("robot_task_executor_node scaffold started")
        self.get_logger().info(f"task_request_topic: {self.task_request_topic}")
        self.get_logger().info(f"task_status_topic: {self.task_status_topic}")
        self.get_logger().info(f"nav_to_pose_action: {self.nav_to_pose_action}")
        self.get_logger().info(
            f"json_fallback: {'enabled' if self.use_json_transport_fallback else 'disabled'}"
        )
        self.get_logger().info("=" * 60)

    def _task_request_callback(self, msg: String) -> None:
        payload = self._safe_parse_json(msg.data)
        self._stats["tasks_received"] += 1

        if payload is None:
            self._publish_status(
                task_id="",
                state="FAILED",
                detail="Invalid JSON task request",
                progress=0.0,
            )
            self._stats["tasks_failed"] += 1
            return

        task_id = str(payload.get("task_id", "")).strip()
        if not task_id:
            task_id = f"task-{int(time.time() * 1000)}"

        # Scaffold behavior: ack task receipt as QUEUED.
        self._publish_status(
            task_id=task_id,
            state="QUEUED",
            detail="Task received by executor scaffold",
            progress=0.0,
        )

    def _tick_callback(self) -> None:
        state = String()
        state.data = json.dumps(self._stats)
        self.state_pub.publish(state)

    def _get_stats_callback(self, request: Trigger.Request, response: Trigger.Response) -> Trigger.Response:
        del request
        response.success = True
        response.message = json.dumps(self._stats)
        return response

    def _publish_status(self, task_id: str, state: str, detail: str, progress: float) -> None:
        msg = String()
        msg.data = json.dumps(
            {
                "task_id": task_id,
                "state": state,
                "detail": detail,
                "progress": float(progress),
                "timestamp_s": time.time(),
            }
        )
        self.status_pub.publish(msg)

    @staticmethod
    def _safe_parse_json(raw: str) -> dict[str, Any] | None:
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None


def main(args: list[str] | None = None) -> None:
    rclpy.init(args=args)
    try:
        node = RobotTaskExecutorNode()
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
