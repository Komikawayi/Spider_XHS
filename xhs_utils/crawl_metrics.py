import json
import math
import os
import threading
import time
from collections import defaultdict, deque
from pathlib import Path


class CrawlMetrics:
    """线程安全的采集运行指标，仅用于观测，不参与调度决策。"""

    def __init__(self):
        self._started_at = time.monotonic()
        self._lock = threading.Lock()
        self._counters = defaultdict(int)
        self._timings = defaultdict(lambda: {"count": 0, "total_seconds": 0.0})
        self._samples = defaultdict(lambda: deque(maxlen=10000))
        self._values = defaultdict(lambda: {"count": 0, "total": 0.0})
        self._value_samples = defaultdict(lambda: deque(maxlen=10000))
        self._max_values = defaultdict(float)
        self._gauges = defaultdict(float)
        self._gauge_max = defaultdict(float)
        self._rates = defaultdict(lambda: {"total": 0, "first": None, "last": None, "buckets": {}})
        self._markers = {}

    def increment(self, name, value=1):
        with self._lock:
            self._counters[name] += value

    def record_duration(self, name, seconds, success=None):
        seconds = max(0.0, float(seconds))
        with self._lock:
            timing = self._timings[name]
            timing["count"] += 1
            timing["total_seconds"] += seconds
            self._samples[name].append(seconds)
            if success is not None:
                outcome = "success" if success else "failure"
                self._counters[f"{name}.{outcome}"] += 1

    def observe_max(self, name, value):
        with self._lock:
            self._max_values[name] = max(self._max_values[name], float(value))

    def record_value(self, name, value):
        value = float(value)
        with self._lock:
            summary = self._values[name]
            summary["count"] += 1
            summary["total"] += value
            self._value_samples[name].append(value)

    def change_gauge(self, name, delta):
        with self._lock:
            self._gauges[name] += float(delta)
            self._gauge_max[name] = max(self._gauge_max[name], self._gauges[name])
            return self._gauges[name]

    def mark_event(self, name, value=1):
        now = time.monotonic()
        bucket = int(now)
        with self._lock:
            rate = self._rates[name]
            rate["total"] += value
            rate["first"] = now if rate["first"] is None else rate["first"]
            rate["last"] = now
            rate["buckets"][bucket] = rate["buckets"].get(bucket, 0) + value

    def mark(self, name, values=None):
        with self._lock:
            if name in self._markers:
                return
            marker = {"elapsed_seconds": round(time.monotonic() - self._started_at, 6)}
            marker.update(values or {})
            self._markers[name] = marker

    def get_counter(self, name):
        with self._lock:
            return self._counters.get(name, 0)

    def snapshot(self):
        with self._lock:
            def percentile(values, ratio):
                if not values:
                    return 0.0
                ordered = sorted(values)
                index = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * ratio) - 1))
                return round(ordered[index], 6)

            timings = {
                name: {
                    "count": values["count"],
                    "total_seconds": round(values["total_seconds"], 6),
                    "average_seconds": round(
                        values["total_seconds"] / values["count"], 6
                    ) if values["count"] else 0.0,
                    "min_seconds": round(min(self._samples[name]), 6) if self._samples[name] else 0.0,
                    "max_seconds": round(max(self._samples[name]), 6) if self._samples[name] else 0.0,
                    "p50_seconds": percentile(self._samples[name], 0.50),
                    "p95_seconds": percentile(self._samples[name], 0.95),
                    "p99_seconds": percentile(self._samples[name], 0.99),
                }
                for name, values in self._timings.items()
            }
            values = {
                name: {
                    "count": summary["count"],
                    "average": round(summary["total"] / summary["count"], 6),
                    "min": round(min(self._value_samples[name]), 6),
                    "max": round(max(self._value_samples[name]), 6),
                    "p50": percentile(self._value_samples[name], 0.50),
                    "p95": percentile(self._value_samples[name], 0.95),
                    "p99": percentile(self._value_samples[name], 0.99),
                }
                for name, summary in self._values.items()
                if summary["count"]
            }
            rates = {
                name: {
                    "total": rate["total"],
                    "average_per_second": round(
                        rate["total"] / max(0.001, (rate["last"] or 0) - (rate["first"] or 0)), 6
                    ),
                    "peak_per_second": max(rate["buckets"].values(), default=0),
                    "active_seconds": round(max(0.0, (rate["last"] or 0) - (rate["first"] or 0)), 6),
                }
                for name, rate in self._rates.items()
            }
            return {
                "elapsed_seconds": round(time.monotonic() - self._started_at, 6),
                "counters": dict(self._counters),
                "timings": timings,
                "values": values,
                "max_values": dict(self._max_values),
                "gauges": {
                    name: {"current": value, "max": self._gauge_max[name]}
                    for name, value in self._gauges.items()
                },
                "rates": rates,
                "markers": dict(self._markers),
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
