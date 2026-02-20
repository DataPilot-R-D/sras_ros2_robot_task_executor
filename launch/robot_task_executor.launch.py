"""Launch file for robot_task_executor_node."""

import os

from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description() -> LaunchDescription:
    pkg_dir = get_package_share_directory("sras_robot_task_executor")
    config_file = os.path.join(pkg_dir, "config", "robot_task_executor.yaml")

    return LaunchDescription(
        [
            DeclareLaunchArgument(
                "config",
                default_value=config_file,
                description="Path to executor config YAML",
            ),
            Node(
                package="sras_robot_task_executor",
                executable="robot_task_executor_node",
                name="robot_task_executor_node",
                output="screen",
                parameters=[LaunchConfiguration("config")],
                emulate_tty=True,
            ),
        ]
    )
