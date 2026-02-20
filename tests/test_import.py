import unittest

try:
    from sras_robot_task_executor.robot_task_executor_node import RobotTaskExecutorNode
except ModuleNotFoundError:
    RobotTaskExecutorNode = None


@unittest.skipIf(RobotTaskExecutorNode is None, "rclpy not installed")
class ImportTests(unittest.TestCase):
    def test_symbol_exported(self) -> None:
        self.assertIsNotNone(RobotTaskExecutorNode)
