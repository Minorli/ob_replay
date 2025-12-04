"""
Oracle DB Replay 管理：开启/结束捕获，并导出捕获文件供 OMA 分析。

注意：
- 需要在 Oracle 端预先创建 DIRECTORY 并赋权给捕获账号。
- 捕获与导出会占用一定存储，请在业务低峰操作。
"""

from .models import ExecutionResult
from .oracle_client import OracleClient


def start_capture(ora: OracleClient, directory: str, name: str) -> ExecutionResult:
    """
    开启 DB Replay 捕获，需要 Oracle 已存在 DIRECTORY `directory`，并授权给当前用户。
    """
    sql = """
    BEGIN
        DBMS_WORKLOAD_CAPTURE.START_CAPTURE(name => :name, dir => :dir);
    END;
    """
    return ora.execute(sql, params={"name": name, "dir": directory}, fetch=False)


def finish_capture(ora: OracleClient) -> ExecutionResult:
    """
    停止捕获并结束 session。
    """
    sql = """
    BEGIN
        DBMS_WORKLOAD_CAPTURE.FINISH_CAPTURE;
    END;
    """
    return ora.execute(sql, fetch=False)


def export_capture(ora: OracleClient, directory: str, filename: str) -> ExecutionResult:
    """
    将捕获导出为文件（例如 capture01.dmp），供 OMA/DB Replay 后续分析或回放。
    """
    sql = """
    BEGIN
        DBMS_WORKLOAD_CAPTURE.EXPORT_CAPTURE(
            dir      => :dir,
            filename => :filename
        );
    END;
    """
    return ora.execute(sql, params={"dir": directory, "filename": filename}, fetch=False)
