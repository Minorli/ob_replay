import shlex
import subprocess
from typing import Dict, List, Optional

from .config import OmaConfig, ToolConfig


def run_oma(
    cfg: ToolConfig,
    mode: str,
    from_type: str,
    source_file: Optional[str] = None,
    schemas: Optional[str] = None,
    replay_mode: Optional[str] = None,
    evaluate_mode: Optional[str] = None,
    performance_mode: Optional[bool] = None,
    max_parallel: Optional[int] = None,
    replay_scale: Optional[float] = None,
    report_root: Optional[str] = None,
    extra_args: Optional[str] = None,
    ob_mode: str = "ORACLE",
) -> Dict[str, str]:
    """
    轻量封装 OMA CLI，返回 {"stdout":..., "stderr":..., "returncode":...}
    仅组装常用参数，更多参数可通过 extra_args 透传。
    """
    cmd: List[str] = [cfg.oma.start_script]
    cmd.extend(["--mode", mode])
    cmd.extend(["--from-type", from_type])
    cmd.extend(["--ob-mode", ob_mode])
    if schemas:
        cmd.extend(["--schemas", schemas])
    if source_file:
        cmd.extend(["--source-file", source_file])
    if replay_mode:
        cmd.extend(["--replay-mode", replay_mode])
    if evaluate_mode:
        cmd.extend(["--evaluate-mode", evaluate_mode])
    if performance_mode is not None:
        cmd.extend(["--performance-mode", str(performance_mode).lower()])
    if max_parallel:
        cmd.extend(["--max-parallel", str(max_parallel)])
    if replay_scale:
        cmd.extend(["--replay-scale", str(replay_scale)])
    rr = report_root or cfg.oma.report_root
    if rr:
        cmd.extend(["--report-root-path", rr])
    # 目标 OB 连接（使用 config 默认）
    cmd.extend(
        [
            "--target-db-host",
            cfg.oceanbase.host,
            "--target-db-port",
            str(cfg.oceanbase.port),
            "--target-db-user",
            cfg.oceanbase.user + "@" + cfg.oceanbase.tenant,
            "--target-db-password",
            cfg.oceanbase.password,
        ]
    )
    # 如果来源为 DB，自动带入源库连接参数（仅支持 dsn 形如 host:port/service）
    if from_type == "DB":
        host, port, svc = _parse_oracle_source(cfg)
        cmd.extend(
            [
                "--source-db-type",
                "ORACLE",
                "--source-db-host",
                host,
                "--source-db-port",
                str(port),
                "--source-db-service-name",
                svc,
                "--source-db-user",
                cfg.oracle.user,
                "--source-db-password",
                cfg.oracle.password,
            ]
        )
        if schemas:
            cmd.extend(["--schemas", schemas])
    if extra_args:
        cmd.extend(shlex.split(extra_args))

    proc = subprocess.run(cmd, capture_output=True, text=True)
    return {
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "returncode": str(proc.returncode),
        "cmd": " ".join(shlex.quote(c) for c in cmd),
        "warn": _warn_paths(cfg),
    }


def _warn_paths(cfg: ToolConfig) -> Optional[str]:
    msgs = []
    if not cfg.oma.start_script:
        msgs.append("未指定 OMA start.sh 路径，请检查 config.ini [oma].start_script 或 OMA_START_SCRIPT。")
    if not cfg.oma.report_root:
        msgs.append("未指定 OMA report_root，可能使用默认目录，请确认有写权限。")
    return "\n".join(msgs) if msgs else None


def _parse_oracle_source(cfg: ToolConfig):
    """
    从配置获取源库连接，优先使用 source_host/port/service，退化解析 dsn。
    """
    if cfg.oracle.source_host:
        host = cfg.oracle.source_host
        port = cfg.oracle.source_port or 1521
        svc = cfg.oracle.source_service or ""
        return host, port, svc
    return _parse_oracle_dsn(cfg.oracle.dsn)


def _parse_oracle_dsn(dsn: str):
    """
    简单解析 host:port/service 形式的 DSN。
    """
    host = dsn
    port = 1521
    svc = ""
    if ":" in dsn:
        host_part, rest = dsn.split(":", 1)
        host = host_part
        if "/" in rest:
            port_part, svc = rest.split("/", 1)
            try:
                port = int(port_part)
            except ValueError:
                port = 1521
        else:
            try:
                port = int(rest)
            except ValueError:
                port = 1521
    return host, port, svc
