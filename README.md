# 通用 Tushare 数据归档系统

本系统旨在提供一个统一、可扩展、自动化的 Tushare 数据归档解决方案。它支持多种数据归档模式，能够适应不同类型的数据接口，并将所有数据以 Parquet 格式进行分区存储，便于后续的量化分析和研究。

## 核心架构

系统采用统一的、面向对象的架构，其核心是 `BaseArchiver` 抽象基类。所有特定类型的归档器都继承自该基类，共享一套通用的逻辑，包括：

- **统一的日志系统**：所有归档活动都记录在同一个 SQLite 数据库 (`data/logs/request_log.db`) 中，便于监控和调试。
- **统一的数据存储**：所有原始数据都以 Parquet 格式存储在 `data/raw/landing/tushare/{data_type}` 目录下。
- **标准化的分区**：采用自描述的目录结构 (`partition_key=value`)，使得数据易于发现和读取。

## 支持的归档模式

系统通过 `main.py` 脚本提供统一的命令行接口，支持以下几种归档模式 (`--archiver-type`)：

| 归档器类型 | `--archiver-type` | 描述 | 适用场景 |
| :--- | :--- | :--- | :--- |
| **季度归档器** | `period` | 按财报季度 (`YYYYMMDD`) 进行版本化归档。 | 利润表、资产负债表等财报数据。 |
| **日期归档器** | `date` | 按公告日 (`YYYYMMDD`) 逐日归档。 | 分红送股等事件驱动数据。 |
| **快照归档器** | `snapshot` | 每日获取全量数据，生成一个完整的快照。 | 股票列表、交易日历等基础信息。 |
| **交易日归档器** | `trade_date` | 依赖本地交易日历，按交易日 (`YYYYMMDD`) 逐日归档。 | ST股列表等需要按交易日获取的数据。 |
| **股票驱动归档器**| `stock_driven` | 依赖本地股票列表，按股票代码 (`ts_code`) 逐一归档。 | 管理层持股等需要按股票代码查询的数据。 |

---

## 使用方法

所有操作都通过 `main.py` 脚本执行。核心参数包括：
- `--archiver-type`: 选择使用的归档器类型。
- `--data-type`: 指定要获取的 Tushare 数据接口名 (例如 `income`, `stock_basic`)。
- `--mode`: 指定运行模式 (`backfill`, `incremental`/`update`, `summary`)。

### 常用命令示例

**1. 快照数据 (Snapshot)**

- **获取交易日历 (`trade_cal`)**
  ```bash
  python main.py --archiver-type snapshot --data-type trade_cal --mode update
  ```
- **获取股票基础信息 (`stock_basic`)**
  ```bash
  python main.py --archiver-type snapshot --data-type stock_basic --mode update
  ```

**2. 季度归档 (Period-based)**

- **回填利润表 (`income`) 历史数据**
  ```bash
  python main.py --archiver-type period --data-type income --mode backfill
  ```
- **增量更新利润表 (最近12个季度)**
  ```bash
  python main.py --archiver-type period --data-type income --mode incremental
  ```

**3. 日期归档 (Date-based)**

- **回填分红送股 (`dividend`) 数据**
  ```bash
  python main.py --archiver-type date --data-type dividend --mode backfill --start-date 20070101
  ```

**4. 交易日归档 (Trade-date-based)**

- **回填ST股列表 (`stock_st`) 数据 (依赖 `trade_cal` 快照)**
  ```bash
  python main.py --archiver-type trade_date --data-type stock_st --mode backfill --start-date 20160101
  ```
- **回填沪深港通成份股 (`stock_hsgt_hk_sz`)**
  ```bash
  python main.py --archiver-type trade_date --data-type stock_hsgt_hk_sz --mode backfill --start-date 20250812
  ```

**5. 股票驱动归档 (Stock-driven)**

- **回填管理层持股 (`stk_rewards`) 数据 (依赖 `stock_basic` 快照)**
  ```bash
  python main.py --archiver-type stock_driven --data-type stk_rewards --mode backfill
  ```

**6. 查看摘要和日志**

- **查看 `income` 数据的摘要和最近日志**
  ```bash
  python main.py --archiver-type period --data_type income --mode summary
  ```
- **查看 `stock_basic` 数据的摘要和最近日志**
  ```bash
  python main.py --archiver-type snapshot --data_type stock_basic --mode summary
  ```






