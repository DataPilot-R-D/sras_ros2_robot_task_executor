#!/usr/bin/env python3
"""ROS2 scaffold node for Robot Task Executor."""

from __future__ import annotations

import json
import math
import time
from typing import Any

from action_msgs.msg import GoalStatus
from nav2_msgs.action import NavigateThroughPoses, NavigateToPose
from geometry_msgs.msg import PoseStamped, Quaternion
from nav_msgs.msg import OccupancyGrid
import rclpy
from rclpy.action import ActionClient
from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile, ReliabilityPolicy
from std_msgs.msg import String
from std_srvs.srv import Trigger
from tf2_msgs.msg import TFMessage

from sras_robot_task_executor.execution_core import (
    CommandRejectedError,
    QueueFullError,
    StatusEvent,
    TaskExecutionCore,
    TaskValidationError,
)

ALERT_STATES = {
    "BLOCKED",
    "FAILED",
    "CANCELED",
    "PAUSED",
    "REDEFINED",
    "REJECTED",
}


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
        self.declare_parameter("map_topic", "/map")
        self.declare_parameter("map_stale_timeout_s", 5.0)
        self.declare_parameter("require_tf", True)
        self.declare_parameter("tf_topic", "/tf")
        self.declare_parameter("tf_static_topic", "/tf_static")
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
        self.action_server_wait_timeout_s = max(
            0.1,
            float(self.get_parameter("action_server_wait_timeout_s").value),
        )
        self.goal_timeout_s = max(1.0, float(self.get_parameter("goal_timeout_s").value))
        self.cancel_timeout_s = max(0.1, float(self.get_parameter("cancel_timeout_s").value))

        self.tick_hz = max(0.1, float(self.get_parameter("executor_tick_hz").value))
        self.max_queue_size = max(1, int(self.get_parameter("max_queue_size").value))
        self.allow_preemption = bool(self.get_parameter("allow_preemption").value)
        self.require_map = bool(self.get_parameter("require_map").value)
        self.map_topic = str(self.get_parameter("map_topic").value)
        self.map_stale_timeout_s = max(0.1, float(self.get_parameter("map_stale_timeout_s").value))
        self.require_tf = bool(self.get_parameter("require_tf").value)
        self.tf_topic = str(self.get_parameter("tf_topic").value)
        self.tf_static_topic = str(self.get_parameter("tf_static_topic").value)
        self.tf_stale_timeout_s = max(0.1, float(self.get_parameter("tf_stale_timeout_s").value))
        self.require_nav_ready = bool(self.get_parameter("require_nav_ready").value)
        self.use_json_transport_fallback = bool(
            self.get_parameter("use_json_transport_fallback").value
        )

    def _setup_ros(self) -> None:
        self._last_map_msg_s: float | None = None
        self._last_tf_msg_s: float | None = None
        self._last_tf_static_msg_s: float | None = None
        self._pending_goal_task_id: str | None = None
        self._pending_goal_future = None
        self._pending_cancel_task_ids: set[str] = set()
        self._active_goal_task_id: str | None = None
        self._active_goal_handle = None
        self._active_goal_sent_s: float | None = None

        self.core = TaskExecutionCore(
            max_queue_size=self.max_queue_size,
            allow_preemption=self.allow_preemption,
            require_map=self.require_map,
            require_tf=self.require_tf,
            require_nav_ready=self.require_nav_ready,
        )
        self.nav_to_pose_client = ActionClient(self, NavigateToPose, self.nav_to_pose_action)
        self.nav_through_poses_client = ActionClient(
            self,
            NavigateThroughPoses,
            self.nav_through_poses_action,
        )
        if self.require_nav_ready:
            to_pose_ready = self.nav_to_pose_client.wait_for_server(
                timeout_sec=self.action_server_wait_timeout_s
            )
            through_poses_ready = self.nav_through_poses_client.wait_for_server(
                timeout_sec=self.action_server_wait_timeout_s
            )
            if not (to_pose_ready and through_poses_ready):
                self.get_logger().warning(
                    "Nav2 action servers not ready at startup; dispatch will stay BLOCKED until ready"
                )

        if self.require_map:
            # Nav2 map_server commonly publishes /map as a transient-local latched topic.
            map_qos = QoSProfile(
                history=HistoryPolicy.KEEP_LAST,
                depth=1,
                reliability=ReliabilityPolicy.RELIABLE,
                durability=DurabilityPolicy.TRANSIENT_LOCAL,
            )
            self.map_sub = self.create_subscription(
                OccupancyGrid,
                self.map_topic,
                self._map_callback,
                map_qos,
            )
        else:
            self.map_sub = None

        if self.require_tf:
            self.tf_sub = self.create_subscription(
                TFMessage,
                self.tf_topic,
                self._tf_callback,
                50,
            )
            self.tf_static_sub = self.create_subscription(
                TFMessage,
                self.tf_static_topic,
                self._tf_static_callback,
                10,
            )
        else:
            self.tf_sub = None
            self.tf_static_sub = None

        self.task_sub = self.create_subscription(
            String,
            self.task_request_topic,
            self._task_request_callback,
            10,
        )
        self.command_sub = self.create_subscription(
            String,
            self.set_task_state_topic,
            self._task_command_callback,
            10,
        )
        self.status_pub = self.create_publisher(String, self.task_status_topic, 10)
        self.alert_pub = self.create_publisher(String, self.alerts_topic, 10)
        self.state_pub = self.create_publisher(String, self.executor_state_topic, 10)
        self.stats_srv = self.create_service(Trigger, "~/get_stats", self._get_stats_callback)
        self.tick_timer = self.create_timer(1.0 / self.tick_hz, self._tick_callback)

    def _log_startup(self) -> None:
        self.get_logger().info("=" * 60)
        self.get_logger().info("robot_task_executor_node scaffold started")
        self.get_logger().info(f"task_request_topic: {self.task_request_topic}")
        self.get_logger().info(f"task_status_topic: {self.task_status_topic}")
        self.get_logger().info(f"nav_to_pose_action: {self.nav_to_pose_action}")
        self.get_logger().info(f"require_map: {self.require_map} (topic={self.map_topic})")
        self.get_logger().info(f"require_tf: {self.require_tf} (topic={self.tf_topic})")
        self.get_logger().info(f"require_nav_ready: {self.require_nav_ready}")
        self.get_logger().info(
            f"json_fallback: {'enabled' if self.use_json_transport_fallback else 'disabled'}"
        )
        self.get_logger().info("=" * 60)

    def _task_request_callback(self, msg: String) -> None:
        payload = self._safe_parse_json(msg.data)

        if payload is None:
            self._publish_status(
                task_id="",
                state="FAILED",
                detail="Invalid JSON task request",
                progress=0.0,
            )
            self._publish_alert(
                task_id="",
                state="FAILED",
                detail="Invalid JSON task request payload",
                severity="warning",
            )
            return

        try:
            queued = self.core.enqueue_task(payload)
        except (TaskValidationError, QueueFullError) as exc:
            task_id = str(payload.get("task_id", "")).strip()
            self._publish_status(
                task_id=task_id,
                state="FAILED",
                detail=f"Task rejected: {exc}",
                progress=0.0,
            )
            self._publish_alert(
                task_id=task_id,
                state="FAILED",
                detail=f"Task rejected: {exc}",
                severity="warning",
            )
            return

        self._publish_event(queued)

    def _task_command_callback(self, msg: String) -> None:
        payload = self._safe_parse_json(msg.data)
        if payload is None:
            self._publish_status(
                task_id="",
                state="REJECTED",
                detail="Invalid JSON command payload",
                progress=0.0,
            )
            self._publish_alert(
                task_id="",
                state="REJECTED",
                detail="Invalid JSON command payload",
                severity="warning",
            )
            return

        command = str(payload.get("command", "")).strip().lower()
        normalized_command = "cancel" if command == "stop" else command
        try:
            event = self.core.handle_command(payload)
        except CommandRejectedError as exc:
            self._publish_status(
                task_id=str(payload.get("task_id", "")).strip(),
                state="REJECTED",
                detail=f"Command rejected: {exc}",
                progress=0.0,
            )
            self._publish_alert(
                task_id=str(payload.get("task_id", "")).strip(),
                state="REJECTED",
                detail=f"Command rejected: {exc}",
                severity="warning",
            )
            return

        self._publish_event(event)
        if event.state == "DISPATCHED":
            self._dispatch_active_task_to_nav()
        if (
            normalized_command in {"cancel", "pause"}
            and self._active_goal_task_id is not None
            and self._active_goal_task_id == event.task_id
        ):
            self._cancel_active_nav_goal(reason=f"operator command: {normalized_command}")
        if (
            normalized_command in {"cancel", "pause"}
            and self._pending_goal_task_id is not None
            and self._pending_goal_task_id == event.task_id
        ):
            # Goal request has been sent but goal handle is not available yet.
            # Defer cancellation and apply it immediately after goal response.
            self._pending_cancel_task_ids.add(event.task_id)

    def _tick_callback(self) -> None:
        self._refresh_dispatch_readiness()
        self._enforce_goal_timeout()

        dispatched = self.core.dispatch_next()
        if dispatched is not None:
            self._publish_event(dispatched)
            if dispatched.state == "DISPATCHED":
                self._dispatch_active_task_to_nav()

        state = String()
        state.data = json.dumps(self.core.snapshot())
        self.state_pub.publish(state)

    def _get_stats_callback(self, request: Trigger.Request, response: Trigger.Response) -> Trigger.Response:
        del request
        response.success = True
        response.message = json.dumps(self.core.snapshot())
        return response

    def _publish_event(self, event: StatusEvent) -> None:
        self._publish_status(
            task_id=event.task_id,
            state=event.state,
            detail=event.detail,
            progress=event.progress,
        )
        if event.state in ALERT_STATES:
            self._publish_alert(
                task_id=event.task_id,
                state=event.state,
                detail=event.detail,
                severity=self._severity_for_state(event.state),
            )

    def _publish_alert(self, task_id: str, state: str, detail: str, severity: str) -> None:
        msg = String()
        msg.data = json.dumps(
            {
                "task_id": task_id,
                "state": state,
                "severity": severity,
                "detail": detail,
                "timestamp_s": time.time(),
            }
        )
        self.alert_pub.publish(msg)

    @staticmethod
    def _severity_for_state(state: str) -> str:
        if state in {"FAILED", "BLOCKED", "REJECTED"}:
            return "warning"
        return "info"

    def _dispatch_active_task_to_nav(self) -> None:
        active_task = self.core.active_task
        if active_task is None:
            return

        try:
            if active_task.nav_action == "navigate_to_pose":
                goal_msg = NavigateToPose.Goal()
                goal_msg.pose = self._build_pose(active_task.raw_payload["goal"])
                send_goal_future = self.nav_to_pose_client.send_goal_async(
                    goal_msg,
                    feedback_callback=(
                        lambda feedback_msg, task_id=active_task.task_id: self._on_nav_feedback(
                            task_id,
                            feedback_msg,
                        )
                    ),
                )
            elif active_task.nav_action == "navigate_through_poses":
                goal_msg = NavigateThroughPoses.Goal()
                goal_msg.poses = [
                    self._build_pose(pose_dict) for pose_dict in active_task.raw_payload["poses"]
                ]
                send_goal_future = self.nav_through_poses_client.send_goal_async(
                    goal_msg,
                    feedback_callback=(
                        lambda feedback_msg, task_id=active_task.task_id: self._on_nav_feedback(
                            task_id,
                            feedback_msg,
                        )
                    ),
                )
            else:
                raise RuntimeError(f"Unsupported nav action mapping: {active_task.nav_action}")
        except Exception as exc:
            self._mark_task_failed_if_active(
                active_task.task_id,
                f"Failed to send Nav2 goal: {exc}",
            )
            return

        self._pending_goal_task_id = active_task.task_id
        self._pending_goal_future = send_goal_future
        self._active_goal_sent_s = time.monotonic()
        send_goal_future.add_done_callback(
            lambda future, task_id=active_task.task_id: self._on_nav_goal_response(task_id, future)
        )

    def _on_nav_goal_response(self, task_id: str, future: Any) -> None:
        cancel_requested_while_pending = task_id in self._pending_cancel_task_ids
        if cancel_requested_while_pending:
            self._pending_cancel_task_ids.discard(task_id)
        self._clear_pending_goal_tracking()

        try:
            goal_handle = future.result()
        except Exception as exc:
            if self.core.active_task_id == task_id:
                self._mark_task_failed_if_active(task_id, f"Nav2 goal response failed: {exc}")
            return

        if not goal_handle.accepted:
            if self.core.active_task_id == task_id:
                self._mark_task_failed_if_active(task_id, "Nav2 goal rejected by action server")
            return

        if cancel_requested_while_pending:
            self._cancel_goal_handle(
                goal_handle,
                reason="operator command while waiting for goal response",
            )
            return

        if self.core.active_task_id != task_id:
            self._cancel_goal_handle(
                goal_handle,
                reason="task no longer active when goal response arrived",
            )
            return

        self._active_goal_task_id = task_id
        self._active_goal_handle = goal_handle
        result_future = goal_handle.get_result_async()
        result_future.add_done_callback(
            lambda result_fut, current_task_id=task_id: self._on_nav_result(
                current_task_id,
                result_fut,
            )
        )
        try:
            self._publish_event(self.core.mark_active())
        except CommandRejectedError:
            return

    def _on_nav_result(self, task_id: str, result_future: Any) -> None:
        if self._active_goal_task_id == task_id:
            self._clear_active_goal_tracking()
        if self.core.active_task_id != task_id:
            return

        try:
            wrapped_result = result_future.result()
        except Exception as exc:
            self._mark_task_failed_if_active(task_id, f"Nav2 result failed: {exc}")
            return

        terminal_state, detail = self._map_nav_result_status(wrapped_result.status)
        try:
            event = self.core.mark_terminal(terminal_state, detail)
        except CommandRejectedError:
            return
        self._publish_event(event)

    def _on_nav_feedback(self, task_id: str, feedback_msg: Any) -> None:
        if self.core.active_task_id != task_id:
            return

        feedback = getattr(feedback_msg, "feedback", None)
        if feedback is None:
            detail = "Nav2 feedback received"
        else:
            distance_remaining = getattr(feedback, "distance_remaining", None)
            if distance_remaining is None:
                detail = "Nav2 feedback received"
            else:
                detail = f"Nav2 distance_remaining={float(distance_remaining):.3f}"

        self._publish_status(
            task_id=task_id,
            state="ACTIVE",
            detail=detail,
            progress=0.0,
        )

    def _mark_task_failed_if_active(self, task_id: str, detail: str) -> None:
        if self.core.active_task_id != task_id:
            return
        self._clear_active_goal_tracking()
        try:
            event = self.core.mark_terminal("FAILED", detail)
        except CommandRejectedError:
            return
        self._publish_event(event)

    def _enforce_goal_timeout(self) -> None:
        if self.core.active_task_id is None:
            return
        if self._active_goal_sent_s is None:
            return
        elapsed_s = time.monotonic() - self._active_goal_sent_s
        if elapsed_s < self.goal_timeout_s:
            return

        active_task_id = self.core.active_task_id
        if active_task_id is None:
            return

        self._cancel_active_nav_goal(reason=f"goal timeout ({self.goal_timeout_s:.1f}s)")
        try:
            timeout_event = self.core.mark_terminal(
                "FAILED",
                f"Goal timeout exceeded after {self.goal_timeout_s:.1f}s",
            )
        except CommandRejectedError:
            return
        self._publish_event(timeout_event)

    def _cancel_active_nav_goal(self, reason: str) -> None:
        if self._active_goal_handle is None:
            return
        self._cancel_goal_handle(self._active_goal_handle, reason=reason)
        self._clear_active_goal_tracking()

    def _cancel_goal_handle(self, goal_handle: Any, reason: str) -> None:
        try:
            cancel_future = goal_handle.cancel_goal_async()
            cancel_future.add_done_callback(
                lambda _: self.get_logger().info(f"Active goal cancel requested ({reason})")
            )
        except Exception as exc:
            self.get_logger().warning(f"Failed to request active goal cancel ({reason}): {exc}")

    def _clear_active_goal_tracking(self) -> None:
        self._active_goal_handle = None
        self._active_goal_task_id = None
        self._active_goal_sent_s = None

    def _clear_pending_goal_tracking(self) -> None:
        self._pending_goal_task_id = None
        self._pending_goal_future = None
        if self._active_goal_task_id is None:
            self._active_goal_sent_s = None

    def _build_pose(self, pose_payload: dict[str, Any]) -> PoseStamped:
        pose = PoseStamped()
        pose.header.frame_id = str(pose_payload.get("frame_id", "map"))
        pose.header.stamp = self.get_clock().now().to_msg()
        pose.pose.position.x = float(pose_payload.get("x", 0.0))
        pose.pose.position.y = float(pose_payload.get("y", 0.0))
        pose.pose.position.z = float(pose_payload.get("z", 0.0))

        yaw = float(pose_payload.get("yaw", 0.0))
        pose.pose.orientation = self._quaternion_from_yaw(yaw)
        return pose

    @staticmethod
    def _quaternion_from_yaw(yaw: float) -> Quaternion:
        half_yaw = 0.5 * yaw
        return Quaternion(
            x=0.0,
            y=0.0,
            z=math.sin(half_yaw),
            w=math.cos(half_yaw),
        )

    @staticmethod
    def _map_nav_result_status(status_code: int) -> tuple[str, str]:
        if status_code == GoalStatus.STATUS_SUCCEEDED:
            return "SUCCEEDED", "Nav2 goal succeeded"
        if status_code == GoalStatus.STATUS_CANCELED:
            return "CANCELED", "Nav2 goal canceled"
        return "FAILED", f"Nav2 goal failed (status={status_code})"

    def _map_callback(self, msg: OccupancyGrid) -> None:
        del msg
        self._last_map_msg_s = time.monotonic()

    def _tf_callback(self, msg: TFMessage) -> None:
        del msg
        self._last_tf_msg_s = time.monotonic()

    def _tf_static_callback(self, msg: TFMessage) -> None:
        del msg
        self._last_tf_static_msg_s = time.monotonic()

    def _refresh_dispatch_readiness(self) -> None:
        now_s = time.monotonic()
        # Treat /map as latched static readiness: once a map is received, readiness stays true.
        map_ready = self._last_map_msg_s is not None
        tf_latest_s: float | None = None
        if self._last_tf_msg_s is not None and self._last_tf_static_msg_s is not None:
            tf_latest_s = max(self._last_tf_msg_s, self._last_tf_static_msg_s)
        else:
            tf_latest_s = self._last_tf_msg_s or self._last_tf_static_msg_s
        tf_ready = self._is_fresh(tf_latest_s, now_s, self.tf_stale_timeout_s)
        nav_ready = self._nav_action_servers_ready()

        self.core.update_readiness(
            map_ready=(not self.require_map) or map_ready,
            tf_ready=(not self.require_tf) or tf_ready,
            nav_ready=nav_ready,
        )

    @staticmethod
    def _is_fresh(last_seen_s: float | None, now_s: float, stale_timeout_s: float) -> bool:
        if last_seen_s is None:
            return False
        return (now_s - last_seen_s) <= stale_timeout_s

    def _nav_action_servers_ready(self) -> bool:
        if not self.require_nav_ready:
            return True

        to_pose_ready = self.nav_to_pose_client.wait_for_server(timeout_sec=0.0)
        through_poses_ready = self.nav_through_poses_client.wait_for_server(timeout_sec=0.0)
        return to_pose_ready and through_poses_ready

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
