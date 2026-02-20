#!/usr/bin/env python3
"""Fake Nav2 action servers for integration testing."""

from __future__ import annotations

import argparse
import json
import time

from nav2_msgs.action import NavigateThroughPoses, NavigateToPose
import rclpy
from rclpy.action import ActionServer, CancelResponse, GoalResponse
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from std_msgs.msg import String


class FakeNav2ActionServer(Node):
    """Serves NavigateToPose and NavigateThroughPoses with deterministic timing."""

    def __init__(self, execute_delay_s: float, goal_response_delay_s: float) -> None:
        super().__init__("fake_nav2_action_server")
        self._execute_delay_s = max(0.1, float(execute_delay_s))
        self._goal_response_delay_s = max(0.0, float(goal_response_delay_s))
        self._feedback_period_s = 0.1
        self._events_pub = self.create_publisher(String, "/fake_nav2/goal_events", 20)

        self._nav_to_pose_server = ActionServer(
            self,
            NavigateToPose,
            "/navigate_to_pose",
            execute_callback=self._execute_to_pose,
            goal_callback=self._goal_callback,
            cancel_callback=self._cancel_callback,
        )
        self._nav_through_poses_server = ActionServer(
            self,
            NavigateThroughPoses,
            "/navigate_through_poses",
            execute_callback=self._execute_through_poses,
            goal_callback=self._goal_callback,
            cancel_callback=self._cancel_callback,
        )
        self.get_logger().info("Fake Nav2 action servers ready")

    def _goal_callback(self, _goal_request: object) -> GoalResponse:
        if self._goal_response_delay_s > 0.0:
            time.sleep(self._goal_response_delay_s)
        return GoalResponse.ACCEPT

    @staticmethod
    def _cancel_callback(_goal_handle: object) -> CancelResponse:
        return CancelResponse.ACCEPT

    def _execute_to_pose(self, goal_handle: object) -> NavigateToPose.Result:
        return self._execute_with_feedback(
            goal_handle=goal_handle,
            feedback_type=NavigateToPose.Feedback,
            result_type=NavigateToPose.Result,
            action_name="navigate_to_pose",
        )

    def _execute_through_poses(self, goal_handle: object) -> NavigateThroughPoses.Result:
        return self._execute_with_feedback(
            goal_handle=goal_handle,
            feedback_type=NavigateThroughPoses.Feedback,
            result_type=NavigateThroughPoses.Result,
            action_name="navigate_through_poses",
        )

    def _execute_with_feedback(
        self,
        goal_handle: object,
        feedback_type: type,
        result_type: type,
        action_name: str,
    ) -> object:
        started_s = time.monotonic()
        target_x = self._extract_target_x(goal_handle)
        self._publish_event(action_name=action_name, event="started", target_x=target_x)
        while (time.monotonic() - started_s) < self._execute_delay_s:
            if goal_handle.is_cancel_requested:
                goal_handle.canceled()
                self._publish_event(action_name=action_name, event="canceled", target_x=target_x)
                return result_type()

            feedback = feedback_type()
            remaining_s = max(0.0, self._execute_delay_s - (time.monotonic() - started_s))
            if hasattr(feedback, "distance_remaining"):
                setattr(feedback, "distance_remaining", float(remaining_s))
            goal_handle.publish_feedback(feedback)
            time.sleep(self._feedback_period_s)

        goal_handle.succeed()
        self._publish_event(action_name=action_name, event="succeeded", target_x=target_x)
        return result_type()

    def _extract_target_x(self, goal_handle: object) -> float:
        request = getattr(goal_handle, "request", None)
        if request is None:
            return 0.0
        if hasattr(request, "pose") and hasattr(request.pose, "pose"):
            return float(request.pose.pose.position.x)
        if hasattr(request, "poses") and request.poses:
            return float(request.poses[0].pose.position.x)
        return 0.0

    def _publish_event(self, action_name: str, event: str, target_x: float) -> None:
        msg = String()
        msg.data = json.dumps(
            {
                "action": action_name,
                "event": event,
                "target_x": float(target_x),
                "timestamp_s": time.time(),
            }
        )
        self._events_pub.publish(msg)

    def destroy_node(self) -> bool:
        self._nav_to_pose_server.destroy()
        self._nav_through_poses_server.destroy()
        return super().destroy_node()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run fake Nav2 action servers.")
    parser.add_argument(
        "--execute-delay-s",
        type=float,
        default=1.5,
        help="Delay before returning SUCCEEDED result for goals.",
    )
    parser.add_argument(
        "--goal-response-delay-s",
        type=float,
        default=0.0,
        help="Delay before accepting/rejecting goals.",
    )
    args = parser.parse_args()

    rclpy.init()
    node = FakeNav2ActionServer(
        execute_delay_s=args.execute_delay_s,
        goal_response_delay_s=args.goal_response_delay_s,
    )
    executor = MultiThreadedExecutor(num_threads=2)
    executor.add_node(node)

    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
    finally:
        executor.remove_node(node)
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
