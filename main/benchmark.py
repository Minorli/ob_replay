import math
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

from .models import BenchmarkResult
from .oceanbase_client import OceanBaseClient


def run_benchmark(
    ob_client: OceanBaseClient,
    sql: str,
    iterations: int = 3,
    concurrency: int = 1,
    oracle_baseline_ms: Optional[float] = None,
) -> BenchmarkResult:
    samples_ms: List[float] = []
    errors: List[str] = []
    lock = threading.Lock()

    def _run_once() -> None:
        start = time.perf_counter()
        result = ob_client.execute(sql)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        with lock:
            if result.success:
                samples_ms.append(elapsed_ms)
            else:
                samples_ms.append(elapsed_ms)
                errors.append(result.error_message or "unknown error")

    if concurrency <= 1:
        for _ in range(iterations):
            _run_once()
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(_run_once) for _ in range(iterations)]
            for future in as_completed(futures):
                future.result()

    successes = iterations - len(errors)
    failures = len(errors)
    avg_ms = statistics.mean(samples_ms) if samples_ms else 0.0
    p95_ms = _percentile(samples_ms, 95) if samples_ms else 0.0

    return BenchmarkResult(
        sql=sql,
        iterations=iterations,
        concurrency=concurrency,
        avg_ms=avg_ms,
        p95_ms=p95_ms,
        successes=successes,
        failures=failures,
        samples_ms=samples_ms,
        errors=errors,
        oracle_baseline_ms=oracle_baseline_ms,
    )


def _percentile(data: List[float], percentile: float) -> float:
    if not data:
        return 0.0
    ordered = sorted(data)
    k = (len(ordered) - 1) * (percentile / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return ordered[int(k)]
    d0 = ordered[int(f)] * (c - k)
    d1 = ordered[int(c)] * (k - f)
    return d0 + d1
