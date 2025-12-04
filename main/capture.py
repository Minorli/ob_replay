import datetime
import json
import time
from typing import Dict, Iterable, List, Optional, Tuple

from .oracle_client import OracleClient


def stream_sqls(
    ora_client: OracleClient,
    output_file: str,
    duration_seconds: int = 3600,
    interval_seconds: int = 5,
    limit_per_interval: int = 200,
    include_binds: bool = True,
    dedup: bool = False,
    include_schemas: Optional[Iterable[str]] = None,
    include_modules: Optional[Iterable[str]] = None,
    default_include_all: bool = True,
) -> int:
    """
    按固定间隔从 Oracle v$sql 抽取最近活跃的 SQL，包含绑定变量信息（若有）。
    可选过滤 schema/module，支持去重，附带执行统计（elapsed/cpu/buffer/disk/rows/fetches/executions）。

    写入 JSON Lines，格式：
    {"sql_id": "...", "child_number": 0, "schema": "...", "module": "...",
     "last_active_time": "2024-05-01T12:00:00", "sql_text": "...",
     "binds": [{"position":1,...}],
     "executions": 10, "avg_elapsed_ms": 1.23, "elapsed_time_us": 12345, "cpu_time_us": 10000,
     "buffer_gets": 100, "disk_reads": 10, "rows_processed": 20, "fetches": 2}

    返回捕获到的 SQL 数量（写入行数）。
    """
    end_time = time.time() + duration_seconds
    last_time = _get_db_time(ora_client) - datetime.timedelta(seconds=interval_seconds)
    seen_keys: set = set()
    count = 0
    include_schemas_set = {s.lower() for s in include_schemas} if include_schemas else None
    include_modules_set = {m.lower() for m in include_modules} if include_modules else None

    with open(output_file, "a", encoding="utf-8") as fp:
        while time.time() < end_time:
            rows = _fetch_sqls_since(ora_client, last_time, limit_per_interval)
            if rows:
                last_time = rows[-1][4]
            for row in rows:
                (
                    sql_id,
                    child,
                    schema,
                    module,
                    last_active,
                    sql_text,
                    elapsed_us,
                    executions,
                    cpu_us,
                    buffer_gets,
                    disk_reads,
                    rows_proc,
                    fetches,
                ) = row
                if include_schemas_set is not None:
                    if (schema or "").lower() not in include_schemas_set:
                        continue
                elif not default_include_all:
                    continue
                if include_modules_set and (module or "").lower() not in include_modules_set:
                    continue
                key = (sql_id, child, last_active) if dedup else None
                if dedup and key in seen_keys:
                    continue
                binds = _fetch_binds(ora_client, sql_id, child) if include_binds else []
                avg_elapsed_ms = None
                if executions and executions > 0 and elapsed_us is not None:
                    avg_elapsed_ms = float(elapsed_us) / 1000.0 / float(executions)
                record = {
                    "sql_id": sql_id,
                    "child_number": child,
                    "schema": schema,
                    "module": module,
                    "last_active_time": _fmt_time(last_active),
                    "sql_text": sql_text,
                    "binds": binds,
                    "executions": executions,
                    "avg_elapsed_ms": avg_elapsed_ms,
                    "elapsed_time_us": elapsed_us,
                    "cpu_time_us": cpu_us,
                    "buffer_gets": buffer_gets,
                    "disk_reads": disk_reads,
                    "rows_processed": rows_proc,
                    "fetches": fetches,
                }
                fp.write(json.dumps(record, ensure_ascii=False) + "\n")
                fp.flush()
                if dedup and key:
                    seen_keys.add(key)
                count += 1
            time.sleep(interval_seconds)
    return count


def _fetch_sqls_since(
    ora_client: OracleClient, last_time: datetime.datetime, limit: int
) -> List[Tuple]:
    stmt = """
    SELECT sql_id,
           child_number,
           parsing_schema_name,
           module,
           last_active_time,
           sql_text,
           elapsed_time,
           executions,
           cpu_time,
           buffer_gets,
           disk_reads,
           rows_processed,
           fetches
      FROM (
            SELECT sql_id,
                   child_number,
                   parsing_schema_name,
                   module,
                   last_active_time,
                   sql_text,
                   elapsed_time,
                   executions,
                   cpu_time,
                   buffer_gets,
                   disk_reads,
                   rows_processed,
                   fetches
              FROM v$sql
             WHERE last_active_time > :last_time
             ORDER BY last_active_time
           )
     WHERE ROWNUM <= :limit
    """
    result = ora_client.execute(
        stmt, params={"last_time": last_time, "limit": limit}, fetch=True
    )
    return result.rows or []


def _fetch_binds(ora_client: OracleClient, sql_id: str, child: int) -> List[Dict]:
    stmt = """
    SELECT position, name, datatype_string, value_string
      FROM v$sql_bind_capture
     WHERE sql_id = :sql_id
       AND child_number = :child
     ORDER BY position
    """
    result = ora_client.execute(
        stmt, params={"sql_id": sql_id, "child": child}, fetch=True
    )
    binds: List[Dict] = []
    if result.success and result.rows:
        for pos, name, dtype, val in result.rows:
            binds.append(
                {
                    "position": pos,
                    "name": name,
                    "datatype": dtype,
                    "value": val,
                }
            )
    return binds


def _get_db_time(ora_client: OracleClient) -> datetime.datetime:
    result = ora_client.execute("SELECT SYSDATE FROM dual", fetch=True)
    if result.success and result.rows:
        return result.rows[0][0]
    return datetime.datetime.now()


def _fmt_time(value) -> str:
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    return str(value)


def load_sqls_from_jsonl(path: str) -> List[str]:
    """
    从 capture 输出的 JSONL 文件提取 sql_text 列表（不带绑定变量替换）。
    """
    sqls: List[str] = []
    with open(path, "r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                sql_text = obj.get("sql_text")
                if sql_text:
                    sqls.append(str(sql_text).strip())
            except json.JSONDecodeError:
                continue
    return sqls
