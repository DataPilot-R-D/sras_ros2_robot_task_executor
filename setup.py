from setuptools import find_packages, setup
import os
from glob import glob

package_name = "sras_robot_task_executor"

setup(
    name=package_name,
    version="0.0.0",
    packages=find_packages(exclude=["test"]),
    data_files=[
        ("share/ament_index/resource_index/packages", ["resource/" + package_name]),
        ("share/" + package_name, ["package.xml"]),
        (os.path.join("share", package_name, "launch"), glob("launch/*.launch.py")),
        (os.path.join("share", package_name, "config"), glob("config/*.yaml")),
    ],
    install_requires=["setuptools"],
    zip_safe=True,
    maintainer="DataPilot R&D",
    maintainer_email="dev@datapilot.dev",
    description="SRAS Reasoning Layer robot task executor node",
    license="Proprietary",
    extras_require={"test": ["pytest"]},
    entry_points={
        "console_scripts": [
            "robot_task_executor_node = sras_robot_task_executor.robot_task_executor_node:main",
        ],
    },
)
