from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ExecutionResult:
    sql: str
    success: bool
    elapsed_ms: float
    rows: Optional[List[Any]] = None
    error_message: Optional[str] = None
    plan: Optional[str] = None
    raw_output: Optional[str] = None


@dataclass
class CompatibilityIssue:
    sql: str
    is_supported: bool
    stage: str
    error_message: Optional[str] = None
    hint: Optional[str] = None
    plan: Optional[str] = None


@dataclass
class BenchmarkResult:
    sql: str
    iterations: int
    concurrency: int
    avg_ms: float
    p95_ms: float
    successes: int
    failures: int
    samples_ms: List[float] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    oracle_baseline_ms: Optional[float] = None
