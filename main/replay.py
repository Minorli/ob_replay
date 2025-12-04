import os
import shlex
import subprocess
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .benchmark import run_benchmark
from .compatibility import check_sql
from .models import BenchmarkResult, CompatibilityIssue
from .oceanbase_client import OceanBaseClient
from .oracle_client import OracleClient


def run_offline(
    ob_client: OceanBaseClient,
    capture_dir: str,
    mode: str = "compat",
    concurrency: int = 1,
    iterations: int = 1,
    oma_cli: Optional[str] = "oma",
    oma_extra: Optional[str] = None,
    sqls_file: Optional[str] = None,
    sqls_format: str = "lines",
) -> Dict[str, Sequence]:
    """
    离线模式：给定 DB Replay 捕获目录，调用 OMA 分析后，在 OB 上做兼容/性能评估。
    约定：OMA 输出的 SQL 文本存放在 capture_dir/sqls.txt（每行一条 SQL）。

    sqls_file: 可指定自定义 SQL 文件，格式默认 lines（每行一条 SQL），也支持 jsonl（capture 输出）。
    mode: compat 或 perf。
    """
    oma_output: Optional[str] = None
    if oma_cli and not sqls_file:
        cmd = [oma_cli, "analyze", "--input", capture_dir]
        if oma_extra:
            cmd.extend(shlex.split(oma_extra))
        proc = subprocess.run(cmd, capture_output=True, text=True)
        oma_output = (proc.stdout or "") + (proc.stderr or "")

    sql_file = sqls_file or os.path.join(capture_dir, "sqls.txt")
    sqls = _read_sqls(sql_file, sqls_format)

    compat_results: List[CompatibilityIssue] = []
    bench_results: List[BenchmarkResult] = []

    if mode == "compat":
        for sql in sqls:
            compat_results.append(check_sql(ob_client, sql, execute=False))
    else:
        for sql in sqls:
            bench_results.append(
                run_benchmark(
                    ob_client,
                    sql,
                    iterations=iterations,
                    concurrency=concurrency,
                    oracle_baseline_ms=None,
                )
            )

    return {
        "sqls": sqls,
        "compat": compat_results,
        "bench": bench_results,
        "oma_output": oma_output,
    }


def run_online(
    ora_client: OracleClient,
    ob_client: OceanBaseClient,
    limit: int = 20,
    mode: str = "compat",
    concurrency: int = 1,
    iterations: int = 1,
    store_file: Optional[str] = None,
    schemas: Optional[Sequence[str]] = None,
    modules: Optional[Sequence[str]] = None,
) -> Dict[str, Sequence]:
    """
    在线模式：实时从 Oracle 抓取最近 SQL（v$sql），可落盘，再在 OB 回放。
    mode: compat 或 perf。
    """
    sqls = fetch_recent_sqls(ora_client, limit=limit, schemas=schemas, modules=modules)
    if store_file:
        with open(store_file, "w") as fp:
            for sql in sqls:
                fp.write(sql + "\n")

    compat_results: List[CompatibilityIssue] = []
    bench_results: List[BenchmarkResult] = []

    if mode == "compat":
        for sql in sqls:
            compat_results.append(check_sql(ob_client, sql, execute=False))
    else:
        for sql in sqls:
            bench_results.append(
                run_benchmark(
                    ob_client,
                    sql,
                    iterations=iterations,
                    concurrency=concurrency,
                    oracle_baseline_ms=None,
                )
            )

    return {
        "sqls": sqls,
        "compat": compat_results,
        "bench": bench_results,
    }


def fetch_recent_sqls(
    ora_client: OracleClient,
    limit: int = 20,
    schemas: Optional[Sequence[str]] = None,
    modules: Optional[Sequence[str]] = None,
) -> List[str]:
    """
    简单从 v$sql 抓取最近活跃的 SQL 文本，排除空 SQL；可按 schema/module 过滤。
    """
    where_filters = ["sql_text IS NOT NULL"]
    params: Dict[str, Any] = {"limit": limit}
    if schemas:
        where_filters.append("parsing_schema_name IN (%s)" % _list_to_binds("sch", schemas, params))
    if modules:
        where_filters.append("module IN (%s)" % _list_to_binds("mod", modules, params))
    where_clause = " AND ".join(where_filters)
    stmt = f"""
    SELECT sql_text FROM (
        SELECT sql_text
        FROM v$sql
        WHERE {where_clause}
        ORDER BY last_active_time DESC
    )
    WHERE ROWNUM <= :limit
    """
    result = ora_client.execute(stmt, params=params, fetch=True)
    sqls: List[str] = []
    if result.success and result.rows:
        for row in result.rows:
            sql_text = row[0]
            if sql_text:
                sqls.append(str(sql_text).strip())
    return sqls


def _list_to_binds(prefix: str, values: Sequence[str], params: Dict[str, Any]) -> str:
    binds = []
    for idx, val in enumerate(values):
        key = f"{prefix}{idx}"
        params[key] = val
        binds.append(f":{key}")
    return ", ".join(binds)


def _read_sql_lines(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    with open(path, "r") as fp:
        lines = [line.strip() for line in fp if line.strip()]
    return lines


def _read_sqls(path: str, fmt: str) -> List[str]:
    if fmt == "jsonl":
        try:
            from .capture import load_sqls_from_jsonl
        except Exception:
            return []
        return load_sqls_from_jsonl(path)
    return _read_sql_lines(path)
