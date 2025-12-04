import json
from typing import Any, Dict

from .models import BenchmarkResult, CompatibilityIssue


def format_compatibility(issue: CompatibilityIssue) -> str:
    parts = [
        "SQL: %s" % issue.sql,
        "Stage: %s" % issue.stage,
        "Supported: %s" % ("YES" if issue.is_supported else "NO"),
    ]
    if issue.error_message:
        parts.append("Error: %s" % issue.error_message.strip())
    if issue.hint:
        parts.append("Hint: %s" % issue.hint)
    if issue.plan:
        parts.append("Plan:\n%s" % issue.plan.strip())
    return "\n".join(parts)


def format_benchmark(result: BenchmarkResult) -> str:
    parts = [
        "SQL: %s" % result.sql,
        "Iterations: %s" % result.iterations,
        "Concurrency: %s" % result.concurrency,
        "Avg: %.2f ms" % result.avg_ms,
        "P95: %.2f ms" % result.p95_ms,
        "Successes: %s Failures: %s" % (result.successes, result.failures),
    ]
    if result.oracle_baseline_ms is not None:
        delta = result.avg_ms - result.oracle_baseline_ms
        parts.append(
            "Oracle baseline: %.2f ms (delta: %+0.2f ms)" % (result.oracle_baseline_ms, delta)
        )
    if result.errors:
        parts.append("Errors: %s" % "; ".join(result.errors))
    return "\n".join(parts)


def to_json(data: Any) -> str:
    return json.dumps(_to_dict(data), ensure_ascii=False, indent=2)


def _to_dict(data: Any) -> Dict:
    if hasattr(data, "__dict__"):
        return {
            key: _to_dict(value)
            for key, value in data.__dict__.items()
            if not key.startswith("_")
        }
    if isinstance(data, (list, tuple)):
        return [_to_dict(item) for item in data]
    return data
