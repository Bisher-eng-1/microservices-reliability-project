import threading
from typing import Dict, List, Optional, Tuple, Union


def _escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _labels_key(labels: Optional[Dict[str, str]] = None) -> Tuple[Tuple[str, str], ...]:
    if not labels:
        return ()
    return tuple(sorted((str(k), str(v)) for k, v in labels.items()))


def _format_labels(label_pairs: Tuple[Tuple[str, str], ...]) -> str:
    if not label_pairs:
        return ""
    parts = [f'{k}="{_escape_label_value(v)}"' for k, v in label_pairs]
    return "{" + ",".join(parts) + "}"


class Metric:
    def __init__(self, name: str, docstring: str, metric_type: str):
        self.name = name
        self.docstring = docstring
        self.metric_type = metric_type
        self.lock = threading.Lock()
        self.values: Dict[Tuple[Tuple[str, str], ...], float] = {}

    def to_prometheus_format(self) -> str:
        lines: List[str] = [
            f"# HELP {self.name} {self.docstring}",
            f"# TYPE {self.name} {self.metric_type}",
        ]
        with self.lock:
            if not self.values:
                lines.append(f"{self.name} 0")
            else:
                for label_pairs in sorted(self.values):
                    val = self.values[label_pairs]
                    lines.append(f"{self.name}{_format_labels(label_pairs)} {val}")
        return "\n".join(lines)


class Counter(Metric):
    def __init__(self, name: str, docstring: str):
        super().__init__(name, docstring, "counter")

    def inc(self, amount: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        if amount < 0:
            raise ValueError("Counters can only be incremented by non-negative amounts.")
        key = _labels_key(labels)
        with self.lock:
            self.values[key] = self.values.get(key, 0.0) + amount


class Gauge(Metric):
    def __init__(self, name: str, docstring: str):
        super().__init__(name, docstring, "gauge")

    def set(self, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        key = _labels_key(labels)
        with self.lock:
            self.values[key] = float(value)

    def inc(self, amount: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        key = _labels_key(labels)
        with self.lock:
            self.values[key] = self.values.get(key, 0.0) + amount

    def dec(self, amount: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        key = _labels_key(labels)
        with self.lock:
            self.values[key] = self.values.get(key, 0.0) - amount


class Histogram:
    DEFAULT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

    def __init__(self, name: str, docstring: str, buckets: Optional[Tuple[float, ...]] = None):
        self.name = name
        self.docstring = docstring
        self.buckets = buckets or self.DEFAULT_BUCKETS
        self.lock = threading.Lock()
        self.data: Dict[Tuple[Tuple[str, str], ...], Dict[str, float]] = {}

    def observe(self, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        key = _labels_key(labels)
        with self.lock:
            if key not in self.data:
                entry: Dict[str, float] = {"_sum": 0.0, "_count": 0.0, "le_+Inf": 0.0}
                for b in self.buckets:
                    entry[f"le_{b}"] = 0.0
                self.data[key] = entry
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
            for label_pairs in sorted(self.data):
                entry = self.data[label_pairs]
                base = [f'{k}="{_escape_label_value(v)}"' for k, v in label_pairs]
                for b in self.buckets:
                    lbl = "{" + ",".join(base + [f'le="{b}"']) + "}"
                    lines.append(f"{self.name}_bucket{lbl} {int(entry[f'le_{b}'])}")
                inf_lbl = "{" + ",".join(base + ['le="+Inf"']) + "}"
                lines.append(f"{self.name}_bucket{inf_lbl} {int(entry['le_+Inf'])}")
                sum_lbl = "{" + ",".join(base) + "}" if base else ""
                lines.append(f"{self.name}_count{sum_lbl} {int(entry['_count'])}")
                lines.append(f"{self.name}_sum{sum_lbl} {entry['_sum']:.6f}")
        return "\n".join(lines)


class MetricsRegistry:
    def __init__(self):
        self.metrics: Dict[str, Union[Metric, Histogram]] = {}
        self.lock = threading.Lock()

    def register_counter(self, name: str, docstring: str) -> Counter:
        with self.lock:
            if name not in self.metrics:
                self.metrics[name] = Counter(name, docstring)
            return self.metrics[name]  # type: ignore[return-value]

    def register_gauge(self, name: str, docstring: str) -> Gauge:
        with self.lock:
            if name not in self.metrics:
                self.metrics[name] = Gauge(name, docstring)
            return self.metrics[name]  # type: ignore[return-value]

    def register_histogram(
        self, name: str, docstring: str, buckets: Optional[Tuple[float, ...]] = None
    ) -> Histogram:
        with self.lock:
            if name not in self.metrics:
                self.metrics[name] = Histogram(name, docstring, buckets)
            return self.metrics[name]  # type: ignore[return-value]

    def generate_exposition(self) -> str:
        with self.lock:
            metrics = list(self.metrics.values())
        return "\n\n".join(m.to_prometheus_format() for m in metrics) + "\n"
