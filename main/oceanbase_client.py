import subprocess
import time
from typing import Optional

from .config import OceanBaseConfig
from .models import ExecutionResult


class OceanBaseClient:
    """
    Executes SQL on OceanBase through the `obclient` CLI.
    """

    def __init__(self, config: OceanBaseConfig) -> None:
        self.config = config

    def execute(self, sql: str) -> ExecutionResult:
        cmd = self._build_command()
        start = time.perf_counter()
        proc = subprocess.run(
            cmd,
            input=sql.encode("utf-8"),
            capture_output=True,
            check=False,
        )
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        stdout = proc.stdout.decode("utf-8", errors="replace")
        stderr = proc.stderr.decode("utf-8", errors="replace")
        success = proc.returncode == 0
        error_message = None if success else (stderr or stdout)
        return ExecutionResult(
            sql=sql,
            success=success,
            elapsed_ms=elapsed_ms,
            raw_output=stdout,
            error_message=error_message,
        )

    def explain(self, sql: str) -> ExecutionResult:
        explain_sql = "EXPLAIN " + sql
        result = self.execute(explain_sql)
        result.plan = result.raw_output
        result.sql = sql
        return result

    def _build_command(self) -> list:
        cmd = [
            self.config.obclient_path,
            "-h",
            self.config.host,
            "-P",
            str(self.config.port),
            "-u",
            "%s@%s" % (self.config.user, self.config.tenant),
            "-p%s" % self.config.password,
            "--connect-timeout=%s" % self.config.connect_timeout,
            "-A",
            "-s",
        ]
        if self.config.database:
            cmd.extend(["-D", self.config.database])
        return cmd
