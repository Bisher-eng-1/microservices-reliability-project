"""
Lightweight Prometheus Exposition Engine.
Provides standard Prometheus text-format metrics (/metrics) with zero external dependencies.
Fully thread-safe with support for Counters, Gauges, and Histograms with dimensional labels.
"""
import threading
import time
from typing import Dict, List, Optional, Tuple


class Metric:
    def __init__(self, name: str, docstring: str, metric_type: str):
        self.name = name
        self.docstring = docstring
        self.metric_type = metric_type
        self.lock = threading.Lock()
        # Key: tuple of sorted (label_name, label_value) pairs -> float value
        self.values: Dict[Tuple[Tuple[str, str], ...], float] = {}

    def _labels_key(self, labels: Optional[Dict[str, str]] = None) -> Tuple[Tuple[str, str], ...]:
        if not labels:
            return ()
        return tuple(sorted((str(k), str(v)) for k, v in labels.items()))

    def to_prometheus_format(self) -> str:
        lines: List[str] = [
            f"# HELP {self.name} {self.docstring}",
            f"# TYPE {self.name} {self.metric_type}",
        ]
        with self.lock:
            if not self.values:
                # Output default 0 without labels
                lines.append(f"{self.name} 0")
            else:
                for label_pairs, val in self.values.items():
                    if not label_pairs:
                        lines.append(f"{self.name} {val}")
                    else:
                        label_str = ",".join(f'{k}="{v}"' for k, v in label_pairs)
                        lines.append(f"{self.name}{{{label_str}}} {val}")
        return "\n".join(lines)


class Counter(Metric):
    def __init__(self, name: str, docstring: str):
        super().__init__(name, docstring, "counter")

    def inc(self, amount: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        if amount < 0:
            raise ValueError("Counters can only be incremented by non-negative amounts.")
        key = self._labels_key(labels)
        with self.lock:
            current = self.values.get(key, 0.0)
            self.values[key] = current + amount


class Gauge(Metric):
    def __init__(self, name: str, docstring: str):
        super().__init__(name, docstring, "gauge")

    def set(self, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        key = self._labels_key(labels)
        with self.lock:
            self.values[key] = float(value)

    def inc(self, amount: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        key = self._labels_key(labels)
        with self.lock:
            current = self.values.get(key, 0.0)
            self.values[key] = current + amount

    def dec(self, amount: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        key = self._labels_key(labels)
        with self.lock:
            current = self.values.get(key, 0.0)
            self.values[key] = current - amount


class Histogram:
    """Standard Prometheus Histogram with default latency buckets in seconds."""
    DEFAULT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

    def __init__(self, name: str, docstring: str, buckets: Optional[Tuple[float, ...]] = None):
        self.name = name
        self.docstring = docstring
        self.buckets = buckets or self.DEFAULT_BUCKETS
        self.lock = threading.Lock()
        # labels -> {bucket_le: count, 'count': count, 'sum': sum}
        self.data: Dict[Tuple[Tuple[str, str], ...], Dict[str, float]] = {}

    def _labels_key(self, labels: Optional[Dict[str, str]] = None) -> Tuple[Tuple[str, str], ...]:
        if not labels:
            return ()
        return tuple(sorted((str(k), str(v)) for k, v in labels.items()))

    def observe(self, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        key = self._labels_key(labels)
        with self.lock:
            if key not in self.data:
                self.data[key] = {"_sum": 0.0, "_count": 0.0}
                for b in self.buckets:
                    self.data[key][f"le_{b}"] = 0.0
                self.data[key]["le_+Inf"] = 0.0

            entry = self.data[key]
            entry["_sum"] += value
            entry["_count"] += 1.0
            for b in self.buckets:
                if value <= b:
                    entry[f"le_{b}"] += 1.0
            entry["le_+Inf"] += 1.0

    def to_prometheus_format(self) -> str:
        lines: List[str] = [
            f"# HELP {self.name} {self.docstring}",
            f"# TYPE {self.name} histogram",
        ]
        with self.lock:
            if not self.data:
                lines.append(f"{self.name}_count 0")
                lines.append(f"{self.name}_sum 0")
                return "\n".join(lines)

            for label_pairs, entry in self.data.items():
                base_labels = [f'{k}="{v}"' for k, v in label_pairs]
                for b in self.buckets:
                    bucket_labels = base_labels + [f'le="{b}"']
                    lbl_str = f"{{{','.join(bucket_labels)}}}"
                    lines.append(f"{self.name}_bucket{lbl_str} {int(entry[f'le_{b}'])}")
                
                inf_labels = base_labels + ['le="+Inf"']
                lbl_str = f"{{{','.join(inf_labels)}}}"
                lines.append(f"{self.name}_bucket{lbl_str} {int(entry['le_+Inf'])}")

                sum_labels = f"{{{','.join(base_labels)}}}" if base_labels else ""
                lines.append(f"{self.name}_count{sum_labels} {int(entry['_count'])}")
                lines.append(f"{self.name}_sum{sum_labels} {entry['_sum']:.6f}")

        return "\n".join(lines)


class MetricsRegistry:
    def __init__(self):
        self.metrics: Dict[str, Metric | Histogram] = {}
        self.lock = threading.Lock()

    def register_counter(self, name: str, docstring: str) -> Counter:
        with self.lock:
            if name in self.metrics:
                return self.metrics[name]  # type: ignore
            counter = Counter(name, docstring)
            self.metrics[name] = counter
            return counter

    def register_gauge(self, name: str, docstring: str) -> Gauge:
        with self.lock:
            if name in self.metrics:
                return self.metrics[name]  # type: ignore
            gauge = Gauge(name, docstring)
            self.metrics[name] = gauge
            return gauge

    def register_histogram(self, name: str, docstring: str, buckets: Optional[Tuple[float, ...]] = None) -> Histogram:
        with self.lock:
            if name in self.metrics:
                return self.metrics[name]  # type: ignore
            hist = Histogram(name, docstring, buckets)
            self.metrics[name] = hist
            return hist

    def generate_exposition(self) -> str:
        output: List[str] = []
        with self.lock:
            for metric in self.metrics.values():
                output.append(metric.to_prometheus_format())
        return "\n\n".join(output) + "\n"
