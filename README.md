目标：用 Tushare 数据搭建端到端量化流程——采集/归档→清洗→研究/回测→执行，并支持云端 GPU 训练。
存储策略：Parquet 作为“唯一真实来源”；增量生成一份 Qlib 原生数据供研究/回测；vn.py 用其自带后端（SQLite/Mongo/Postgres）。Qlib 的 E/D 缓存放本机/临时盘。
一致性：交易日历、复权口径、停牌与缺失值处理、代码映射等清洗逻辑统一实现一套，研究与执行两端复用，避免双写。
日常流程：每日收盘增量拉取→写 Parquet→更新 Qlib→导入 vn.py→同步到对象存储；云端训练先把 Qlib 数据下发到节点本地 SSD 再训练。
架构与仓库：先单仓库多模块（core/data/research/execution）+ 三个服务（etl/research_job/trader），用固定的 Schema/路径/信号格式做模块间契约。
产物与对接：研究侧产出信号/目标权重（Parquet/Arrow或消息队列），交易服务消费执行并回写成交与绩效。
约束与权衡：不需要 MySQL 在线读；长期只保留 Parquet + 一份 Qlib 基础数据，缓存可清理，控制空间占用。
需要的支持：提供最小可用的增量 ETL/转换脚本、信号 Schema 模板与一致性校验脚本。


执行计划：
https://tushare.pro/document/2?doc_id=33

历史回填（一次性）
逐季度用 income_vip 抓取 2006Q1 至当前季度；一个季度一次请求、一个季度一个文件，连同请求参数一并保存，便于审计。
目录建议：raw/landing/tushare/income/period=YYYYMMDD/ingest_date=YYYY-MM-DD/… 与 raw/norm/tushare/income/period=YYYYMMDD/ingest_date=YYYY-MM-DD/…
记录 request_log（period, ingest_date, params, row_count, checksum, status）。
日常增量（PIT 友好）
每日执行“滑动窗口”检查：current_period + 过去 N 个季度（建议 N=12；你原先设 8 也可，从 8 起步）。
每个 period 调用 income_vip 后规范化、排序，计算稳定 checksum（如 xxhash64）。与上一版该 period 的 checksum 比较：
未变更：只写日志（request_log），不写新数据文件。
有变更：按当天 ingest_date 追加写一版（Parquet），形成版本留痕。
披露高峰（季度末至法定披露截止）：
提高窗口 N（如 16–24）或提高运行频率；披露截止日后再跑一次“定版”检查。
注意：不做“覆盖”，而是“变更才物化 + 每日留痕”（无变更的日子只有日志，无额外文件）。
调度与任务生成
启动检查：若历史某些 period 缺失，批量回填历史数据（并行逐季度）。
日常：
若处于披露窗口：执行“季度更新 + 回溯 N 个季度”的滑动窗口任务（变更才写）。
非披露窗口：每日执行滑动窗口任务，但通常只有当季或极少 period 有变更。
幂等与失败恢复：批内去重、临时路径写入后原子移动；batch_id=period+ingest_date+run_id；失败批次可重放。
存储与主键
分区：period=end_date，二级分区 ingest_date；不按 ts_code 分区。
原始层保留全字段与多版本行；主键候选：(ts_code, end_date, report_type, comp_type, ann_date, f_ann_date)；不做“取最新”。
压缩：Parquet+zstd/snappy；定期小文件合并（周/旬）。
结果与开销
存储增长可控：只有发生变更的日子才新增文件；多数日子仅日志。
查询：需要“最新”读每个 period 的最大 ingest_date；PIT 按查询时点选择 ≤ 当日的最大 ingest_date 版本。





