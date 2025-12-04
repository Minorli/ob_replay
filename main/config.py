import configparser
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class OracleConfig:
    dsn: str
    user: str
    password: str
    schema: Optional[str] = None
    thick_mode: bool = False
    instant_client_dir: Optional[str] = None
    source_host: Optional[str] = None
    source_port: Optional[int] = None
    source_service: Optional[str] = None


@dataclass
class OceanBaseConfig:
    host: str
    port: int
    tenant: str
    user: str
    password: str
    database: Optional[str] = None
    mode: str = "oracle"
    obclient_path: str = "obclient"
    connect_timeout: int = 15


@dataclass
class OmaConfig:
    start_script: str = "/home/minorli/oma-4.2.5/bin/start.sh"
    report_root: Optional[str] = None


@dataclass
class ToolConfig:
    oracle: OracleConfig
    oceanbase: OceanBaseConfig
    capture_schemas: Optional[list] = None
    oma: OmaConfig = field(default_factory=OmaConfig)


def load_config(path: str) -> ToolConfig:
    parser = configparser.ConfigParser()
    read = parser.read(path)
    if not read:
        raise FileNotFoundError("Config file not found: %s" % path)

    oracle_raw = _section_to_dict(parser, "oracle")
    ob_raw = _section_to_dict(parser, "oceanbase")
    capture_raw = _section_to_dict(parser, "capture")
    oma_raw = _section_to_dict(parser, "oma")

    _require_keys(oracle_raw, ["dsn", "user", "password"], "oracle")
    _require_keys(ob_raw, ["host", "port", "tenant", "user", "password"], "oceanbase")
    capture_schemas = None
    if capture_raw:
        schemas = capture_raw.get("schemas")
        if schemas:
            capture_schemas = [s.strip() for s in schemas.split(",") if s.strip()]

    oma = OmaConfig(
        start_script=oma_raw.get("start_script", "/home/minorli/oma-4.2.5/bin/start.sh"),
        report_root=oma_raw.get("report_root"),
    )

    oracle = OracleConfig(
        dsn=oracle_raw["dsn"],
        user=oracle_raw["user"],
        password=oracle_raw["password"],
        schema=oracle_raw.get("schema"),
        thick_mode=_to_bool(oracle_raw.get("thick_mode", "false")),
        instant_client_dir=oracle_raw.get("instant_client_dir"),
        source_host=oracle_raw.get("source_host"),
        source_port=int(oracle_raw["source_port"]) if "source_port" in oracle_raw else None,
        source_service=oracle_raw.get("source_service"),
    )

    ob = OceanBaseConfig(
        host=ob_raw["host"],
        port=int(ob_raw["port"]),
        tenant=ob_raw["tenant"],
        user=ob_raw["user"],
        password=ob_raw["password"],
        database=ob_raw.get("database"),
        mode=ob_raw.get("mode", "oracle"),
        obclient_path=ob_raw.get("obclient_path", "obclient"),
        connect_timeout=int(ob_raw.get("connect_timeout", 15)),
    )

    return ToolConfig(oracle=oracle, oceanbase=ob, capture_schemas=capture_schemas, oma=oma)


def _section_to_dict(parser: configparser.ConfigParser, section: str) -> Dict[str, str]:
    if not parser.has_section(section):
        return {}
    return {k: v for k, v in parser.items(section)}


def _require_keys(data: Dict[str, Any], keys: Any, section: str) -> None:
    missing = [k for k in keys if k not in data]
    if missing:
        raise ValueError(
            "Missing required config keys in %s: %s" % (section, ", ".join(missing))
        )


def env_override(config: ToolConfig) -> ToolConfig:
    """
    Allow env overrides for credentials to avoid committing secrets.
    """
    oracle_pwd = os.environ.get("ORACLE_PASSWORD")
    ob_pwd = os.environ.get("OB_PASSWORD")
    ora_ic = os.environ.get("ORACLE_INSTANT_CLIENT")
    ora_thick = os.environ.get("ORACLE_THICK_MODE")
    oma_start = os.environ.get("OMA_START_SCRIPT")
    oma_report = os.environ.get("OMA_REPORT_ROOT")
    if oracle_pwd:
        config.oracle.password = oracle_pwd
    if ob_pwd:
        config.oceanbase.password = ob_pwd
    if ora_ic:
        config.oracle.instant_client_dir = ora_ic
    if ora_thick:
        config.oracle.thick_mode = ora_thick.lower() in ("1", "true", "yes", "on")
    if oma_start:
        config.oma.start_script = oma_start
    if oma_report:
        config.oma.report_root = oma_report
    return config


def _to_bool(value: str) -> bool:
    return str(value).lower() in ("1", "true", "yes", "on")
