import time
from typing import Any, Dict, Optional

from .config import OracleConfig
from .models import ExecutionResult


class OracleClient:
    """
    Thin wrapper around python-oracledb / cx_Oracle with timing helpers.
    """

    def __init__(self, config: OracleConfig) -> None:
        self.config = config
        self._driver = None
        self._conn = None

    def connect(self):
        if self._conn:
            return self._conn
        driver = self._load_driver()
        self._conn = driver.connect(
            user=self.config.user,
            password=self.config.password,
            dsn=self.config.dsn,
        )
        if self.config.schema:
            cursor = self._conn.cursor()
            cursor.execute(
                "ALTER SESSION SET CURRENT_SCHEMA = :schema", {"schema": self.config.schema}
            )
            cursor.close()
        return self._conn

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def execute(
        self, sql: str, params: Optional[Dict[str, Any]] = None, fetch: bool = False
    ) -> ExecutionResult:
        conn = self.connect()
        cursor = conn.cursor()
        start = time.perf_counter()
        try:
            cursor.execute(sql, params or {})
            rows = cursor.fetchall() if fetch else None
            conn.commit()
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            return ExecutionResult(
                sql=sql, success=True, elapsed_ms=elapsed_ms, rows=rows
            )
        except Exception as exc:  # pylint: disable=broad-except
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            return ExecutionResult(
                sql=sql,
                success=False,
                elapsed_ms=elapsed_ms,
                error_message=str(exc),
            )
        finally:
            cursor.close()

    def fetch_baseline_ms(self, sql_id: str) -> Optional[float]:
        """
        Fetch average elapsed time for a SQL_ID from V$SQL.
        Returns None if unavailable.
        """
        stmt = """
            SELECT elapsed_time/1000/executions
            FROM v$sql
            WHERE sql_id = :sql_id AND executions > 0
        """
        result = self.execute(stmt, params={"sql_id": sql_id}, fetch=True)
        if result.success and result.rows:
            value = result.rows[0][0]
            if value is None:
                return None
            return float(value)
        return None

    def _load_driver(self):
        if self._driver:
            return self._driver
        try:
            import oracledb as driver
        except ImportError:
            try:
                import cx_Oracle as driver  # type: ignore
            except ImportError as exc:
                raise ImportError(
                    "Install python-oracledb or cx_Oracle to use OracleClient "
                    "(compatible with Python 3.7+)."
                ) from exc
        # 优先使用 python-oracledb thick 模式，支持本地 Instant Client
        if driver.__name__ == "oracledb":
            use_thick = self.config.thick_mode or bool(self.config.instant_client_dir)
            if use_thick:
                try:
                    driver.init_oracle_client(lib_dir=self.config.instant_client_dir)
                except Exception as exc:  # pylint: disable=broad-except
                    raise RuntimeError(
                        "初始化 Oracle thick 模式失败，请确认 instant_client_dir 配置正确。"
                    ) from exc
        self._driver = driver
        return driver
