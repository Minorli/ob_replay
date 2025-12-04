# OB Replay Assistant (Python 3.7+)

轻量级的 Oracle -> OceanBase SQL 回放与优化工具脚手架，专注于三个场景：
- 兼容性检查：用 `EXPLAIN` 判断 SQL 是否能在 OB 上解析/规划，失败时给出错误原因与提示。
- 性能对比：在 OB 上回放 SQL，记录单次/并发耗时，可选对比 Oracle 的历史平均耗时。
- 规则化优化建议：基于 SQL 文本与执行计划的简单启发式规则给出调优建议。

## 快速开始
1) 准备依赖
- Python 3.7+
- `python-oracledb` 或 `cx_Oracle`（仅在需要访问 Oracle 时），若使用 thick 模式需安装 Oracle Instant Client。
- `obclient` 可用且能直连目标 OceanBase。
- 若使用 OMA：确保 `/home/minorli/oma-4.2.5/bin/start.sh` 可执行，日志/报告目录可写。

2) 配置连接  
复制 `config.ini`，填写 Oracle/OB 连接信息（可用环境变量 `ORACLE_PASSWORD`、`OB_PASSWORD` 覆盖密码）。示例已对应
`obclient -uSYS@ob4ora#observer147 -P2883 -pPAssw0rd01## -h172.16.0.147`。实际运行前请根据现场环境更新配置，避免把凭据硬编码进代码。
若需使用 python-oracledb thick 模式，请安装 Oracle Instant Client，并在 `[oracle]` 段设置 `thick_mode=true` 与 `instant_client_dir=/path/to/instantclient`
（或通过环境变量 `ORACLE_THICK_MODE`、`ORACLE_INSTANT_CLIENT` 指定）。

3) 运行示例（按功能模块分类，均可在 `python3 run.py <command> --help` 查看中文示例）
```bash
# 入口脚本（自动使用环境变量 OB_TOOL_CONFIG 或默认 config.ini）
python3 run.py compat --sql "select * from dual"

# 基准测试，5 次，4 并发；可选从 Oracle V$SQL 获取 baseline
python3 run.py benchmark \
  --sql "select count(*) from t" --iterations 5 --concurrency 4 --oracle-sql-id 8abc123def

# 仅生成规则化优化建议（可附带 EXPLAIN 结果文本）
python3 run.py advise \
  --sql "select * from orders where customer_id=1" --plan-file plan.txt

# 在线持续捕获（含绑定变量），例如 12 小时，每 5 秒拉取一次，写入 JSONL
python3 run.py capture --output captured_sqls.jsonl --duration-seconds 43200 --interval-seconds 5 --limit-per-interval 200
# 若需去重/过滤 schema/module 或使用 config.ini 的 capture.schemas：
python3 run.py capture --dedup --schema HR --module APP --output captured_sqls.jsonl
python3 run.py capture --respect-config-schemas --output captured_sqls.jsonl

# 回放/评估（统一入口）
# 1) DB Replay 目录（假定目录下已有 sqls.txt，可选让 OMA 先分析）
python3 run.py replay --source-type dbreplay --source-path /path/to/capture --mode compat
python3 run.py replay --source-type dbreplay --source-path /path/to/capture --mode perf --concurrency 4 --iterations 3
# 2) 捕获 JSONL 或文本行文件
python3 run.py replay --source-type jsonl --source-path captured_sqls.jsonl --mode compat
python3 run.py replay --source-type lines --source-path sqls.txt --mode perf --concurrency 4 --iterations 3
# 3) 在线从 v$sql 抓取后评估
python3 run.py replay --source-type online --limit 30 --mode compat --store-file captured.sqls
python3 run.py replay --source-type online --limit 30 --mode perf --concurrency 4 --iterations 3

# 管理 Oracle DB Replay 捕获（需已有 DIRECTORY 并授权）
python3 run.py dbreplay --action start --dir DBR_DIR --name CAPTURE01
python3 run.py dbreplay --action finish
python3 run.py dbreplay --action export --dir DBR_DIR --filename capture01.dmp

# 调用本地 OMA CLI（/home/minorli/oma-4.2.5/bin/start.sh），封装常用参数
# 离线兼容评估（DB Replay 捕获目录）
python3 run.py oma --mode ANALYZE --from-type DB_REPLAY --source-file /path/to/capture --schemas SCOTT,HR --report-root /path/to/report
# 回放评估（READ 模式，目标 OB 从 config.ini 获取）
python3 run.py oma --mode REPLAY --from-type DB_REPLAY --source-file /path/to/capture --schemas SCOTT,HR --replay-mode READ --max-parallel 50 --report-root /path/to/report
# 直连源库采集并评估（from-type=DB，源库连接自动取 config.ini 的 [oracle] 配置）
python3 run.py oma --mode ANALYZE --from-type DB --schemas OMS_USER --report-root /path/to/report
```

## 模块化用法（按场景）

### 1) 单条 SQL 工具
- 兼容性：`python3 run.py compat --sql "select * from dual"` 或 `--sql-file`
- 性能基准：`python3 run.py benchmark --sql "select count(*) from t" --iterations 5 --concurrency 4`
- 规则化优化建议：`python3 run.py advise --sql "select * from orders where c_id=1" [--plan-file plan.txt]`

### 2) 在线捕获（Python 轮询 v$sql）
- 长时间捕获含绑定：`python3 run.py capture --output captured_sqls.jsonl --duration-seconds 43200 --interval-seconds 5 --limit-per-interval 200`
- 过滤/去重示例：`python3 run.py capture --dedup --schema HR --module APP --output captured_sqls.jsonl`
- 默认使用 config.ini 的 capture.schemas；若需覆盖可直接传 `--schema ...`
- 输出 JSONL 每行包含 SQL、绑定、以及执行统计（executions、avg_elapsed_ms、elapsed_time_us、cpu_time_us、buffer_gets、disk_reads、rows_processed、fetches）。
- 说明：v$sql/v$sql_bind_capture 在高并发下可能采样/覆盖，如需尽量完整的流量建议用 DB Replay。

### 3) 在线捕获（OMA 直连源库采集）
- 兼容/静态性能评估：`python3 run.py oma --mode ANALYZE --from-type DB --schemas <SCHEMA> --report-root <目录>`
- 源库连接取 config.ini 的 [oracle] 配置，可用 `--extra-args` 追加 OMA 原生参数。

### 4) 离线捕获（Oracle DB Replay）
1. 在源库一次性准备：
   ```sql
   CREATE OR REPLACE DIRECTORY DBR_DIR AS '/path/to/replay_dir';
   GRANT READ, WRITE ON DIRECTORY DBR_DIR TO <捕获用户>;
   ```
2. 开启捕获：`python3 run.py dbreplay --action start --dir DBR_DIR --name CAP1`
3. 运行业务流量。
4. 停止捕获：`python3 run.py dbreplay --action finish`
5. 导出捕获：`python3 run.py dbreplay --action export --dir DBR_DIR --filename cap1.dmp`
6. 捕获目录 DBR_DIR（包含 wcr/capture 文件）可交给 OMA 或本工具回放。

### 5) 回放/评估（统一入口 replay）
- 源为 DB Replay 目录（兼容/性能）：
  - 兼容：`python3 run.py replay --source-type dbreplay --source-path /path/cap --mode compat`
  - 性能：`python3 run.py replay --source-type dbreplay --source-path /path/cap --mode perf --concurrency 4 --iterations 3`
  - 可选调用 OMA 先分析目录生成 sqls.txt（默认使用 config.ini 的 [oma].start_script；也可 `--oma-cli` 覆盖）。
- 源为捕获 JSONL：`python3 run.py replay --source-type jsonl --source-path captured_sqls.jsonl --mode compat`（性能同上，加 --mode perf 等）。
- 源为文本行文件：`python3 run.py replay --source-type lines --source-path sqls.txt --mode perf --concurrency 4 --iterations 3`
- 在线抓取后立即评估：`python3 run.py replay --source-type online --limit 30 --mode compat --store-file captured.sqls`；性能模式加 `--mode perf --concurrency ...`
- 性能对比中的 Oracle 基线：
  - online/jsonl/lines：可用 `--baseline-source oracle`（在线查询 v$sql）或 `--baseline-source file --baseline-file baseline.json`（格式 { "sql_text": avg_elapsed_ms }）。
  - dbreplay：可选先用 OMA 分析生成 sqls.txt，再用 replay perf 模式；如需 Oracle 基线，可自备映射文件或在回放前从 Oracle 端导出基线数据。

### 6) OMA 评估/回放（封装 start.sh 常用参数）
- DB Replay 目录，兼容/静态性能评估：  
  `python3 run.py oma --mode ANALYZE --from-type DB_REPLAY --source-file /path/cap --schemas SCOTT,HR --report-root /path/report`
- DB Replay 回放评估：  
  `python3 run.py oma --mode REPLAY --from-type DB_REPLAY --source-file /path/cap --schemas SCOTT,HR --replay-mode READ --max-parallel 50 --report-root /path/report`
- 直连源库采集并评估（不产出 DB Replay）：  
  `python3 run.py oma --mode ANALYZE --from-type DB --schemas OMS_USER --report-root /path/report`
- OMA 路径/报告目录可在 config.ini [oma] 设置或用环境变量/参数覆盖；更多 OMA 原生参数可用 `--extra-args` 透传。

## 目录结构
- `main/config.py`：读取/校验配置（INI/txt），支持环境变量覆盖密码。
- `main/oracle_client.py`：基于 python-oracledb/cx_Oracle 的简单执行器，可从 V$SQL 抓 baseline。
- `main/oceanbase_client.py`：通过 `obclient` 执行/EXPLAIN SQL，记录耗时与错误。
- `main/compatibility.py`：使用 EXPLAIN 判定兼容性，输出错误与提示。
- `main/benchmark.py`：单次或并发回放 SQL，统计 avg/p95、失败原因。
- `main/advisor.py`：规则化建议（SELECT *、函数索引失效、LIKE '%xx%'、全表扫描提示、索引缺失检查等）。
- `main/reporting.py`：格式化输出（文本/JSON）。
- `main/cli.py`：命令行入口。
- `main/dbreplay.py`：Oracle DB Replay 捕获/导出封装，可配合 OMA 使用。
- `main/replay.py`：离线/在线获取 Oracle SQL，调用 OMA 分析后在 OB 上做兼容/性能评估。
- `main/capture.py`：在线轮询 v$sql，持续捕获 SQL（含绑定变量）写入 JSONL。
- `main/oma_runner.py`：封装 OMA start.sh 常用参数调用。

## 典型流程与模式
### 捕获方式
- 离线捕获（Oracle DB Replay）
  1. 在源库创建 DIRECTORY 并授权。
  2. `python3 run.py dbreplay --action start --dir DBR_DIR --name CAP1`
  3. 运行业务流量后，`python3 run.py dbreplay --action finish`
  4. 导出捕获：`python3 run.py dbreplay --action export --dir DBR_DIR --filename cap1.dmp`
  5. 捕获目录 DBR_DIR 可交给 OMA 或 replay 模块评估。

- 在线捕获（Python 轮询 v$sql）
  - 适合快速抽样，含绑定变量：`python3 run.py capture --output captured_sqls.jsonl --duration-seconds 43200 --interval-seconds 5 --limit-per-interval 200`
  - 可用 `--respect-config-schemas` 或 `--schema/--module` 过滤；`--dedup` 去重。
  - 注意 v$sql/v$sql_bind_capture 可能有采样/覆盖，高并发场景若需全量仍建议使用 DB Replay。

- 在线捕获（OMA 直连源库采集）
  - 使用 `oma --mode ANALYZE --from-type DB`，源库连接取自 config.ini 的 [oracle]，可指定 `--schemas`，报告输出到 `--report-root`。

### 评估/回放方式
- 本工具回放（replay）
  - 源为 DB Replay 目录：`python3 run.py replay --source-type dbreplay --source-path /path/cap --mode compat`（兼容，仅 EXPLAIN）
  - 性能模式：`--mode perf --concurrency N --iterations M`
  - 源为 JSONL/lines：`--source-type jsonl/lines --source-path <file>`
  - 源为在线抓取：`--source-type online --limit 30 ...`

- OMA 评估/回放
  - 兼容/静态性能评估（DB Replay）：`python3 run.py oma --mode ANALYZE --from-type DB_REPLAY --source-file /path/cap --schemas SCOTT,HR --report-root /path/report`
  - 回放评估：`python3 run.py oma --mode REPLAY --from-type DB_REPLAY --source-file /path/cap --schemas SCOTT,HR --replay-mode READ --max-parallel 50 --report-root /path/report`
  - 直连源库评估：`python3 run.py oma --mode ANALYZE --from-type DB --schemas OMS_USER --report-root /path/report`

- 单条 SQL 工具
  - `compat`：快速 EXPLAIN 兼容性。
  - `benchmark`：单条 SQL 性能（可并发）。
  - `advise`：规则化优化建议（SQL/PLAN）。

### 设计思路
- 工具作为“胶水层”：自动化“捕获/抽样 -> OB 回放/评估 -> 报告”，高级语法/调优可委托 OMA/OCP/LLM。
- 当前性能回放使用线程池，可扩展为多 SQL 场景或接入 Locust。
- OMA 路径与报告目录可在 config.ini 的 [oma] 设置，或通过命令行/环境变量覆盖。

## 关于官方工具与集成
- OceanBase 官方的 OMA/OMS/OCP 功能全面，若环境允许，可用 Python 直接调度这些工具生成报告，把结果融合到本工具输出。
- 在仅有 CLI 的场景，本脚手架仍可独立运行，提供最小可用的兼容性校验与性能对比。

## 提示
- 对 DML/DDL 使用 `--execute` 或压测前请确认目标库数据安全。
- `obclient` 执行时会把密码作为命令行参数，生产环境请改为读取环境变量或使用交互式输入。
