import json
import os
import threading
import time
from collections import defaultdict
from pathlib import Path


class CrawlMetrics:
    """线程安全的采集运行指标，仅用于观测，不参与调度决策。"""

    def __init__(self):
        self._started_at = time.monotonic()
        self._lock = threading.Lock()
        self._counters = defaultdict(int)
        self._timings = defaultdict(lambda: {"count": 0, "total_seconds": 0.0})
        self._max_values = defaultdict(float)

    def increment(self, name, value=1):
        with self._lock:
            self._counters[name] += value

    def record_duration(self, name, seconds, success=None):
        seconds = max(0.0, float(seconds))
        with self._lock:
            timing = self._timings[name]
            timing["count"] += 1
            timing["total_seconds"] += seconds
            if success is not None:
                outcome = "success" if success else "failure"
                self._counters[f"{name}.{outcome}"] += 1

    def observe_max(self, name, value):
        with self._lock:
            self._max_values[name] = max(self._max_values[name], float(value))

    def snapshot(self):
        with self._lock:
            timings = {
                name: {
                    "count": values["count"],
                    "total_seconds": round(values["total_seconds"], 6),
                    "average_seconds": round(
                        values["total_seconds"] / values["count"], 6
                    ) if values["count"] else 0.0,
                }
                for name, values in self._timings.items()
            }
            return {
                "elapsed_seconds": round(time.monotonic() - self._started_at, 6),
                "counters": dict(self._counters),
                "timings": timings,
                "max_values": dict(self._max_values),
            }

    def log_summary(self, logger):
        snapshot = self.snapshot()
        logger.info(
            "性能指标: elapsed={:.2f}s, counters={}, timings={}, max={}".format(
                snapshot["elapsed_seconds"],
                snapshot["counters"],
                snapshot["timings"],
                snapshot["max_values"],
            )
        )

    def write_json(self, file_path, metadata=None):
        payload = self.snapshot()
        if metadata:
            payload["metadata"] = metadata
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        temp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temp_path, path)
        return str(path)
