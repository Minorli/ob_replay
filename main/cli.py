import argparse
import sys
from typing import Optional

from . import (
    advisor,
    benchmark,
    capture,
    compatibility,
    dbreplay,
    oma_runner,
    replay,
    reporting,
)
from .config import env_override, load_config
from .oceanbase_client import OceanBaseClient
from .oracle_client import OracleClient


def main(argv: Optional[list] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    cfg = env_override(load_config(args.config))
    ob_client = OceanBaseClient(cfg.oceanbase)

    if args.command == "compat":
        sql = _read_sql(args)
        issue = compatibility.check_sql(ob_client, sql, execute=args.execute)
        print(reporting.format_compatibility(issue))
        return 0 if issue.is_supported else 1

    if args.command == "benchmark":
        sql = _read_sql(args)
        oracle_baseline = None
        if args.oracle_sql_id:
            oracle_baseline = _fetch_oracle_baseline(cfg, args.oracle_sql_id)
        result = benchmark.run_benchmark(
            ob_client,
            sql,
            iterations=args.iterations,
            concurrency=args.concurrency,
            oracle_baseline_ms=oracle_baseline,
        )
        print(reporting.format_benchmark(result))
        return 0 if result.failures == 0 else 1

    if args.command == "advise":
        sql = _read_sql(args)
        plan = None
        if args.plan_file:
            with open(args.plan_file, "r") as fp:
                plan = fp.read()
        tips = advisor.advise(sql, plan=plan)
        print("SQL:", sql)
        if plan:
            print("Plan:\n%s" % plan)
        print("Suggestions:")
        for idx, tip in enumerate(tips, start=1):
            print("%s. %s" % (idx, tip))
        return 0

    if args.command == "dbreplay":
        return _run_dbreplay(cfg, args)

    if args.command == "replay":
        return _run_replay(cfg, args)

    if args.command == "capture":
        return _run_capture(cfg, args)

    if args.command == "oma":
        return _run_oma(cfg, args)

    parser.print_help()
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Oracle->OceanBase 回放与评估工具",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--config",
        default="config.ini",
        help="Path to config (INI/txt) with Oracle and OceanBase credentials.",
    )
    sub = parser.add_subparsers(dest="command")

    compat = sub.add_parser(
        "compat",
        help="单条 SQL 兼容性检查（EXPLAIN）。",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""示例:
  python3 run.py compat --sql "select * from dual"
  python3 run.py compat --sql-file /tmp/sql.txt""",
    )
    compat.add_argument("--sql", help="SQL text to check.")
    compat.add_argument("--sql-file", help="Read SQL from file.")
    compat.add_argument(
        "--execute",
        action="store_true",
        help="Execute the SQL after EXPLAIN (use only for safe/read-only SQL).",
    )

    bench = sub.add_parser(
        "benchmark",
        help="单条 SQL 性能基准（可并发）。",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""示例:
  python3 run.py benchmark --sql "select count(*) from t" --iterations 5 --concurrency 4
  python3 run.py benchmark --sql-file /tmp/sql.txt --iterations 10""",
    )
    bench.add_argument("--sql", help="SQL text to run.")
    bench.add_argument("--sql-file", help="Read SQL from file.")
    bench.add_argument("--iterations", type=int, default=3, help="Run count.")
    bench.add_argument("--concurrency", type=int, default=1, help="Concurrent workers.")
    bench.add_argument(
        "--oracle-sql-id",
        help="Optional Oracle SQL_ID to fetch baseline from V$SQL (requires Oracle connectivity).",
    )

    advise_cmd = sub.add_parser(
        "advise",
        help="规则化优化建议（SQL/PLAN）。",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""示例:
  python3 run.py advise --sql "select * from orders where c_id=1"
  python3 run.py advise --sql-file /tmp/sql.txt --plan-file /tmp/plan.txt""",
    )
    advise_cmd.add_argument("--sql", help="SQL text.")
    advise_cmd.add_argument("--sql-file", help="Read SQL from file.")
    advise_cmd.add_argument("--plan-file", help="Path to an EXPLAIN plan text file.")

    dbrep = sub.add_parser(
        "dbreplay",
        help="管理 Oracle DB Replay 捕获（start/finish/export）。",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""示例:
  python3 run.py dbreplay --action start --dir DBR_DIR --name CAP1
  python3 run.py dbreplay --action finish
  python3 run.py dbreplay --action export --dir DBR_DIR --filename cap1.dmp""",
    )
    dbrep.add_argument(
        "--action",
        choices=["start", "finish", "export"],
        required=True,
        help="start: 开启捕获; finish: 停止捕获; export: 导出捕获文件。",
    )
    dbrep.add_argument("--dir", help="Oracle DIRECTORY 名称（start/export 需要）。")
    dbrep.add_argument("--name", help="捕获名称（start 需要）。")
    dbrep.add_argument("--filename", help="导出文件名，如 capture01.dmp（export 需要）。")

    replay_cmd = sub.add_parser(
        "replay",
        help="回放/评估：支持 DB Replay 目录、JSONL/行文件、在线 v$sql。",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""示例:
  # DB Replay 目录，兼容性
  python3 run.py replay --source-type dbreplay --source-path /path/cap --mode compat
  # DB Replay 目录，性能（并发4，每条3次）
  python3 run.py replay --source-type dbreplay --source-path /path/cap --mode perf --concurrency 4 --iterations 3
  # 捕获 JSONL
  python3 run.py replay --source-type jsonl --source-path captured_sqls.jsonl --mode compat
  # 在线从 v$sql 抓取 30 条，性能评估
  python3 run.py replay --source-type online --limit 30 --mode perf --concurrency 4 --iterations 3 --store-file grabbed.sqls""",
    )
    replay_cmd.add_argument(
        "--source-type",
        choices=["dbreplay", "jsonl", "lines", "online"],
        required=True,
        help="dbreplay: DB Replay 目录；jsonl/lines: SQL 文件；online: 直接从源库抓取 v$sql。",
    )
    replay_cmd.add_argument(
        "--source-path",
        help="当 source-type 为 dbreplay/jsonl/lines 时的路径（目录或文件）。online 模式无需。",
    )
    replay_cmd.add_argument(
        "--mode",
        choices=["compat", "perf"],
        default="compat",
        help="compat: 仅 EXPLAIN 兼容性；perf: 在 OB 上执行并统计耗时（可指定并发/次数）。",
    )
    replay_cmd.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="性能评估模式的并发数（mode=perf 时生效）。",
    )
    replay_cmd.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="性能评估模式，每条 SQL 的执行次数。",
    )
    replay_cmd.add_argument(
        "--limit",
        type=int,
        default=20,
        help="online 模式：从 v$sql 抓取的 SQL 条数。",
    )
    replay_cmd.add_argument(
        "--schema",
        action="append",
        help="online 模式：按 schema 过滤（可多次传递）。",
    )
    replay_cmd.add_argument(
        "--module",
        action="append",
        help="online 模式：按 module 过滤（可多次传递）。",
    )
    replay_cmd.add_argument(
        "--store-file",
        help="online 模式：将抓取到的 SQL 落盘保存，便于复用。",
    )
    replay_cmd.add_argument(
        "--oma-cli",
        default=None,
        help="dbreplay 模式可选：调用 OMA 分析 DB Replay 目录后读取 sqls.txt，不填默认使用 config.ini 的 oma.start_script。",
    )
    replay_cmd.add_argument(
        "--oma-extra",
        help="dbreplay 模式可选：OMA CLI 额外参数（字符串，将 split 追加）。",
    )

    capture_cmd = sub.add_parser(
        "capture",
        help="在线持续捕获 Oracle v$sql SQL+绑定，写入 JSONL。",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""示例:
  # 12 小时，每5秒轮询，写入 JSONL
  python3 run.py capture --output captured_sqls.jsonl --duration-seconds 43200 --interval-seconds 5 --limit-per-interval 200
  # 使用 config.ini 的 capture.schemas 并去重
  python3 run.py capture --respect-config-schemas --dedup --output captured_sqls.jsonl""",
    )
    capture_cmd.add_argument(
        "--output",
        default="captured_sqls.jsonl",
        help="输出文件路径（JSON Lines，每行一条 SQL+绑定）。",
    )
    capture_cmd.add_argument(
        "--duration-seconds",
        type=int,
        default=3600,
        help="捕获持续时间（秒），例如 43200 表示 12 小时。",
    )
    capture_cmd.add_argument(
        "--interval-seconds",
        type=int,
        default=5,
        help="轮询 v$sql 的时间间隔（秒）。",
    )
    capture_cmd.add_argument(
        "--limit-per-interval",
        type=int,
        default=200,
        help="每次轮询最多抓取的 SQL 行数，用于控制压力。",
    )
    capture_cmd.add_argument(
        "--no-binds",
        action="store_true",
        help="不抓取 v$sql_bind_capture（默认抓取）。",
    )
    capture_cmd.add_argument(
        "--dedup",
        action="store_true",
        help="开启去重（按 sql_id+child_number+last_active_time）。默认关闭以尽量保留每条。",
    )
    capture_cmd.add_argument(
        "--schema",
        action="append",
        help="仅捕获指定 schema（可多次传递）。默认不过滤。",
    )
    capture_cmd.add_argument(
        "--module",
        action="append",
        help="仅捕获指定 module（可多次传递）。默认不过滤。",
    )
    capture_cmd.add_argument(
        "--respect-config-schemas",
        action="store_true",
        help="若配置文件 capture.schemas 存在，则使用该列表作为过滤（默认忽略配置）。",
    )

    oma_cmd = sub.add_parser(
        "oma",
        help="调用 OMA（/home/minorli/oma-4.2.5/bin/start.sh）做评估/回放。",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""示例:
  # DB Replay 目录做兼容/静态性能评估
  python3 run.py oma --mode ANALYZE --from-type DB_REPLAY --source-file /path/cap --schemas SCOTT,HR --report-root /path/report
  # DB Replay 回放评估，READ 模式
  python3 run.py oma --mode REPLAY --from-type DB_REPLAY --source-file /path/cap --schemas SCOTT,HR --replay-mode READ --max-parallel 50 --report-root /path/report
  # 直连源库采集并评估（from-type=DB），源库连接取 config.ini [oracle]
  python3 run.py oma --mode ANALYZE --from-type DB --schemas OMS_USER --report-root /path/report""",
    )
    oma_cmd.add_argument(
        "--mode",
        choices=["ANALYZE", "REPLAY"],
        required=True,
        help="ANALYZE: 兼容/静态性能评估；REPLAY: 回放。",
    )
    oma_cmd.add_argument(
        "--from-type",
        choices=["DB_REPLAY", "TEXT", "DB"],
        required=True,
        help="输入类型：DB_REPLAY 捕获目录，TEXT SQL 文件，DB 直连源库。",
    )
    oma_cmd.add_argument("--source-file", help="当 from-type 为 DB_REPLAY/TEXT 时的输入路径。")
    oma_cmd.add_argument("--schemas", help="逗号分隔的 schema 列表。")
    oma_cmd.add_argument(
        "--replay-mode",
        choices=["READ", "WRITE", "READ_WRITE", "PL", "ALL"],
        help="REPLAY 回放模式。",
    )
    oma_cmd.add_argument(
        "--evaluate-mode",
        choices=[
            "NOOP",
            "ONLY_SOURCE",
            "ONLY_TARGET",
            "ONLY_INSTANCE",
            "SOURCE_TARGET",
            "SOURCE_INSTANCE",
            "APPLICATION_CODE",
        ],
        help="评估模式。",
    )
    oma_cmd.add_argument(
        "--performance-mode",
        action="store_true",
        help="ANALYZE 时开启静态性能评估（默认 false）。",
    )
    oma_cmd.add_argument("--max-parallel", type=int, help="最大回放线程数。")
    oma_cmd.add_argument("--replay-scale", type=float, help="回放倍数，默认 1.0。")
    oma_cmd.add_argument("--report-root", help="报告输出目录，不填则使用 config 里的配置或默认。")
    oma_cmd.add_argument("--extra-args", help="其他参数透传给 OMA（空格分隔）。")

    return parser


def _read_sql(args) -> str:
    if args.sql:
        return args.sql.strip()
    if args.sql_file:
        with open(args.sql_file, "r") as fp:
            return fp.read().strip()
    raise SystemExit("Provide --sql or --sql-file.")


def _fetch_oracle_baseline(cfg, sql_id: str) -> Optional[float]:
    try:
        ora_client = OracleClient(cfg.oracle)
        return ora_client.fetch_baseline_ms(sql_id)
    except Exception as exc:  # pylint: disable=broad-except
        print("WARN: unable to fetch Oracle baseline: %s" % exc, file=sys.stderr)
        return None


def _run_dbreplay(cfg, args) -> int:
    ora = OracleClient(cfg.oracle)
    if args.action == "start":
        if not args.dir or not args.name:
            raise SystemExit("start 需要 --dir 与 --name（Oracle DIRECTORY 名称与捕获名称）。")
        result = dbreplay.start_capture(ora, directory=args.dir, name=args.name)
    elif args.action == "finish":
        result = dbreplay.finish_capture(ora)
    else:  # export
        if not args.dir or not args.filename:
            raise SystemExit("export 需要 --dir 与 --filename。")
        result = dbreplay.export_capture(ora, directory=args.dir, filename=args.filename)

    if result.success:
        print("DB Replay %s 成功，耗时 %.2f ms" % (args.action, result.elapsed_ms))
        return 0
    print("DB Replay %s 失败：%s" % (args.action, result.error_message))
    return 1


def _run_replay(cfg, args) -> int:
    ob_client = OceanBaseClient(cfg.oceanbase)
    mode = args.mode
    source_type = args.source_type
    if source_type in ("dbreplay", "jsonl", "lines"):
        if not args.source_path:
            raise SystemExit("source-type 为 dbreplay/jsonl/lines 时必须提供 --source-path。")
        if source_type == "dbreplay":
            oma_cli = args.oma_cli or cfg.oma.start_script
            result = replay.run_offline(
                ob_client=ob_client,
                capture_dir=args.source_path,
                mode=mode,
                concurrency=args.concurrency,
                iterations=args.iterations,
                oma_cli=oma_cli,
                oma_extra=args.oma_extra,
                sqls_file=None,
                sqls_format="lines",
            )
        else:
            fmt = "jsonl" if source_type == "jsonl" else "lines"
            result = replay.run_offline(
                ob_client=ob_client,
                capture_dir=".",
                mode=mode,
                concurrency=args.concurrency,
                iterations=args.iterations,
                oma_cli=None,
                sqls_file=args.source_path,
                sqls_format=fmt,
            )
    else:
        ora_client = OracleClient(cfg.oracle)
        result = replay.run_online(
            ora_client=ora_client,
            ob_client=ob_client,
            limit=args.limit,
            mode=mode,
            concurrency=args.concurrency,
            iterations=args.iterations,
            store_file=args.store_file,
            schemas=args.schema,
            modules=args.module,
        )

    _print_replay_result(result, compat_only=(mode == "compat"))
    return 0


def _print_replay_result(result, compat_only: bool) -> None:
    sqls = result.get("sqls") or []
    print("捕获 SQL 数量：", len(sqls))
    if compat_only:
        print("兼容评估模式：")
        for idx, issue in enumerate(result.get("compat") or [], start=1):
            status = "OK" if issue.is_supported else "FAIL"
            print("%s. [%s] %s" % (idx, status, issue.sql))
            if issue.error_message:
                print("   错误:", issue.error_message)
            if issue.hint:
                print("   提示:", issue.hint)
    else:
        print("性能评估模式：")
        for idx, bench in enumerate(result.get("bench") or [], start=1):
            print(
                "%s. avg=%.2f ms p95=%.2f ms success=%s fail=%s sql=%s"
                % (idx, bench.avg_ms, bench.p95_ms, bench.successes, bench.failures, bench.sql)
            )
            if bench.errors:
                print("   errors:", "; ".join(bench.errors))

    if result.get("oma_output"):
        print("OMA 输出：")
        print(result["oma_output"])


def _run_capture(cfg, args) -> int:
    ora_client = OracleClient(cfg.oracle)
    include_binds = not args.no_binds
    schemas = args.schema
    if args.respect_config_schemas and cfg.capture_schemas:
        schemas = cfg.capture_schemas if not schemas else schemas + cfg.capture_schemas
    count = capture.stream_sqls(
        ora_client=ora_client,
        output_file=args.output,
        duration_seconds=args.duration_seconds,
        interval_seconds=args.interval_seconds,
        limit_per_interval=args.limit_per_interval,
        include_binds=include_binds,
        dedup=args.dedup,
        include_schemas=schemas,
        include_modules=args.module,
        default_include_all=not bool(schemas),
    )
    print("捕获完成，输出文件 %s，共 %s 条 SQL。" % (args.output, count))
    return 0


def _run_oma(cfg, args) -> int:
    res = oma_runner.run_oma(
        cfg=cfg,
        mode=args.mode,
        from_type=args.from_type,
        source_file=args.source_file,
        schemas=args.schemas,
        replay_mode=args.replay_mode,
        evaluate_mode=args.evaluate_mode,
        performance_mode=args.performance_mode,
        max_parallel=args.max_parallel,
        replay_scale=args.replay_scale,
        report_root=args.report_root,
        extra_args=args.extra_args,
    )
    print("命令:", res.get("cmd"))
    if res.get("warn"):
        print("提示:", res["warn"])
    if res.get("stdout"):
        print("STDOUT:\n", res["stdout"])
    if res.get("stderr"):
        print("STDERR:\n", res["stderr"])
    return int(res.get("returncode", "1"))


if __name__ == "__main__":
    sys.exit(main())
