"""Dashboard notification schema and throttle logic.

Pure-logic module — no ROS imports, fully unit-testable.
Publishes curated, structured notifications to /ui/dashboard_notifications
so the dashboard sees only what matters (intruder detections, plan events,
task state changes, navigation progress).

Canonical source: sras_ros2_robot_task_planner/.../dashboard_notifications.py
Copied here because separate ROS2 packages cannot cross-import without adding
package dependencies.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Notification schema
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DashboardNotification:
    """Single notification destined for the dashboard."""

    category: str
    level: str  # info | warning | error | success
    title: str
    message: str
    task_id: str = ""
    incident_key: str = ""
    timestamp_s: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps({
            "category": self.category,
            "level": self.level,
            "title": self.title,
            "message": self.message,
            "task_id": self.task_id,
            "incident_key": self.incident_key,
            "timestamp_s": self.timestamp_s,
            "metadata": self.metadata,
        })


def build_notification(
    *,
    category: str,
    level: str,
    title: str,
    message: str,
    task_id: str = "",
    incident_key: str = "",
    metadata: dict[str, Any] | None = None,
    now_fn: Any = None,
) -> DashboardNotification:
    """Factory that stamps the notification with the current time."""
    ts = (now_fn or time.time)()
    return DashboardNotification(
        category=category,
        level=level,
        title=title,
        message=message,
        task_id=task_id,
        incident_key=incident_key,
        timestamp_s=ts,
        metadata=metadata or {},
    )


# ---------------------------------------------------------------------------
# Throttle configuration
# ---------------------------------------------------------------------------

# Default per-category dedup/rate-limit windows (seconds).
DEFAULT_THROTTLE_WINDOWS: dict[str, float] = {
    "intruder_detected": 5.0,
    "plan_scheduled": 5.0,
    "task_state_changed": 1.0,
    "robot_action_monitor": 2.0,
}


_DEFAULT_UNKNOWN_WINDOW_S: float = 5.0


@dataclass
class ThrottleConfig:
    """Per-category dedup intervals."""

    windows: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_THROTTLE_WINDOWS))
    max_entries: int = 500


# ---------------------------------------------------------------------------
# Throttle implementation
# ---------------------------------------------------------------------------

def _throttle_key(notification: DashboardNotification) -> str:
    """Derive the dedup key from the notification category + identifiers."""
    cat = notification.category
    if cat == "intruder_detected":
        return f"{cat}:{notification.incident_key}"
    if cat == "plan_scheduled":
        return f"{cat}:{notification.task_id}"
    if cat == "task_state_changed":
        to_state = notification.metadata.get("to_state", "")
        return f"{cat}:{notification.task_id}:{to_state}"
    if cat == "robot_action_monitor":
        return f"{cat}:{notification.task_id}"
    # Fallback: category only
    return cat


class NotificationThrottle:
    """Rate-limits dashboard notifications per category."""

    def __init__(
        self,
        config: ThrottleConfig | None = None,
        now_fn: Any = None,
    ) -> None:
        self._config = config or ThrottleConfig()
        self._now_fn = now_fn or time.time
        self._last_published: dict[str, float] = {}

    def should_publish(self, notification: DashboardNotification) -> bool:
        """Return True if *notification* should be published (not throttled)."""
        self._cleanup_if_needed()

        key = _throttle_key(notification)
        now = self._now_fn()
        window = self._config.windows.get(
            notification.category,
            _DEFAULT_UNKNOWN_WINDOW_S,
        )

        last_ts = self._last_published.get(key)
        if last_ts is not None and (now - last_ts) < window:
            return False

        self._last_published[key] = now
        return True

    def _cleanup_if_needed(self) -> None:
        """Evict oldest entries when the dict exceeds *max_entries*."""
        if len(self._last_published) <= self._config.max_entries:
            return
        sorted_keys = sorted(self._last_published, key=self._last_published.get)  # type: ignore[arg-type]
        to_remove = len(self._last_published) - self._config.max_entries
        for k in sorted_keys[:to_remove]:
            del self._last_published[k]
