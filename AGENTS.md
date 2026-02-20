# Repository Guidelines

## Project Structure & Module Organization
This repository is a ROS 2 Python package (`ament_python`) named `sras_robot_task_executor`.

- `sras_robot_task_executor/`: runtime Python code.
- `sras_robot_task_executor/robot_task_executor_node.py`: main node entry implementation.
- `launch/robot_task_executor.launch.py`: launch description for local execution.
- `config/robot_task_executor.yaml`: default ROS parameters.
- `tests/`: Python test suite (currently a scaffold-level import smoke test).
- `package.xml`, `setup.py`, `resource/`: package metadata and install wiring.

Keep new runtime modules inside `sras_robot_task_executor/`; keep launch and config changes in their dedicated folders.

## Build, Test, and Development Commands
- `colcon build --packages-select sras_robot_task_executor`: build only this package.
- `source install/setup.bash`: load built package into the current shell.
- `ros2 run sras_robot_task_executor robot_task_executor_node`: run node directly.
- `ros2 launch sras_robot_task_executor robot_task_executor.launch.py`: run via launch file with YAML parameters.
- `colcon test --packages-select sras_robot_task_executor`: run package tests.
- `colcon test-result --verbose`: print test summary/failures.
- `python3 -m pytest tests -q`: run Python tests directly during quick iteration.

## Coding Style & Naming Conventions
- Use Python 3 with 4-space indentation and PEP 8/PEP 257 conventions.
- Prefer explicit type hints (as used in the node scaffold).
- Naming: modules/functions/variables in `snake_case`, classes in `PascalCase`.
- Keep ROS parameter declarations centralized in `_declare_parameters()` and mirrored in `config/robot_task_executor.yaml`.
- Keep callbacks focused; move reusable logic to small private helpers.

## Testing Guidelines
- Use `pytest` with files named `test_*.py` under `tests/`.
- Add tests for JSON parsing, parameter handling, and state/status publishing logic when behavior expands.
- For ROS interaction changes (topics/services/actions), prefer adding integration coverage and run via `colcon test`.

## Commit & Pull Request Guidelines
- Follow imperative, scope-first commit subjects, and include issue references when applicable (example in history: `Scaffold robot_task_executor package for issue #44`).
- Recommended pattern: `<Verb> <component> ... for issue #<id>`.
- Keep commits focused and reviewable.
- PRs should include: linked issue, behavior summary, commands used for validation, and notes on changed topics/parameters.

## Security & Configuration Tips
- Do not commit secrets or environment-specific endpoints in `config/*.yaml`.
- Prefer launch-time config overrides (for example `config:=/tmp/local.yaml`) instead of editing defaults for local experiments.
