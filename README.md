# sras_ros2_robot_task_executor

ROS 2 package scaffold for Reasoning Layer issue #44:

- https://github.com/DataPilot-R-D/cosmos-hackathon/issues/44

## Purpose

`robot_task_executor_node` executes `RobotTask` requests using Nav2 actions and publishes execution status.

Default intended execution path:
- `NavigateToPose`
- `NavigateThroughPoses`

## Current state

This repository currently contains an initial scaffold:
- package structure
- launch/config files
- minimal node wiring with JSON fallback placeholders

## Run (scaffold)

```bash
ros2 run sras_robot_task_executor robot_task_executor_node
```

or

```bash
ros2 launch sras_robot_task_executor robot_task_executor.launch.py
```

## Tests

Fast unit tests (no ROS graph needed):

```bash
python3 -m unittest tests/test_execution_core.py -v
```

Launch integration tests (requires ROS 2 + launch_testing):

```bash
colcon test --packages-select sras_robot_task_executor --pytest-args -k executor_launch
colcon test-result --verbose
```
