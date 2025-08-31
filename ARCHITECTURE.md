# 彩元滚滚 - 简化三层数据架构设计

## 总览

本项目采用简化的三层数据架构，兼顾"审计友好、性能可控、PIT合规"的设计原则：

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   RAW Layer     │───▶│   NORM Layer    │───▶│ QLIB-READY Layer│
│  唯一真实来源    │    │  接口级规范化    │    │  Qlib就绪数据    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 三层职责分工

### 1. RAW Layer (原始层)
- **职责**: 唯一真实来源，忠于Tushare原始格式
- **存储**: Parquet格式，包含完整元数据
- **特点**: 永不改写，支持归档与版本管理
- **目录结构**:
  ```
  data/raw/
  ├── landing/tushare/     # 新到达数据
  ├── norm/tushare/        # 标准化存储
  ├── archive/tushare/     # 历史归档
  └── manual/              # 手工数据
  ```

### 2. NORM Layer (规范层)
- **职责**: 接口级规范化，每个接口一份CSV
- **目标**: 审计友好，记录所有数据处理决策
- **产物**: 
  - 标准化CSV文件 (按接口组织)
  - Schema定义文件
  - 决策记录文件 (JSONL格式)
- **目录结构**:
  ```
  data/norm/
  ├── by_interface/
  │   ├── quotes_daily/    # 日频行情
  │   ├── fin_is/          # 利润表  
  │   ├── fin_bs/          # 资产负债表
  │   ├── fin_cf/          # 现金流量表
  │   ├── analyst/         # 分析师预期
  │   ├── corp_actions/    # 公司行为
  │   ├── universe/        # 股票池
  │   ├── calendar/        # 交易日历
  │   └── ref/             # 参考数据
  ├── schemas/             # Schema定义
  └── decisions/           # 处理决策记录
  ```

### 3. DOMAIN Layer (领域层)
- **职责**: 数据域整合与推导，按业务需求组织
- **特点**: 
  - 主存Parquet，性能优先
  - 包含推导字段 (_sc, _sq, _ttm)
  - 支持PIT对齐与前向填充
- **目录结构**:
  ```
  data/domain/
  ├── quotes_day/
  │   ├── events/          # 事件形数据
  │   └── daily_panel/     # 日频面板数据
  ├── financials/
  │   ├── events/          # 财报事件
  │   ├── quarterly_wide/  # 季度宽表
  │   └── daily_panel/     # PIT日频数据
  ├── analyst/
  ├── corp_actions/
  ├── universe/
  ├── calendar/
  └── ref/
  ```

### 4. QLIB-FEED Layer (投喂层)
- **职责**: 按标的拆分CSV，供Qlib dump_bin使用
- **特点**: 临时文件，短期保留
- **目录结构**:
  ```
  data/qlib-feed/
  ├── quotes_day/
  │   ├── 000001.SZ.csv
  │   ├── 600000.SH.csv
  │   └── ...
  └── features/
      ├── 000001.SZ.csv    # 包含财务特征
      ├── 600000.SH.csv
      └── ...
  ```

## 关键契约与约定

### 代码格式标准
- **标准格式**: `000001.SZ` / `600000.SH`
- **交易所映射**: SZ(深交所) / SH(上交所) / BJ(北交所)
- **代码映射表**: `data/norm/by_interface/ref/code_map.csv`

### 主键定义
```python
PRIMARY_KEYS = {
    "quotes_daily": ["symbol", "trade_date"],
    "fin_is/bs/cf": ["symbol", "period_end", "report_type", "update_flag"],
    "analyst": ["symbol", "ann_date", "report_date", "analyst_id"],
    "corp_actions": ["symbol", "ex_date", "action_type"]
}
```

### 财务字段命名约定
- **季累字段**: `revenue_sc` (季累原值)
- **单季字段**: `revenue_sq` (差分计算)
- **TTM字段**: `revenue_ttm` (滚动4季)
- **同比字段**: `revenue_yoy` (同比增长)
- **环比字段**: `revenue_qoq` (环比增长)

### PIT(Point In Time)规则
- **可用日规则**: 公告日后第一个交易日
- **回刷策略**: 财务数据8季度，行情数据120天
- **前向填充**: 在Domain层完成，确保PIT合规

### 去重策略
- **行情数据**: 保留最新更新 (`KEEP_LATEST_UPDATE`)
- **财务数据**: 优先正式报告 (`KEEP_OFFICIAL_REPORT`)
- **分析师数据**: 保留最新更新 (`KEEP_LATEST_UPDATE`)

## 数据质量与审计

### 质量标准
- **空值比例**: ≤ 95%
- **重复比例**: ≤ 1%
- **异常值阈值**: 5倍标准差
- **单季为负比例**: ≤ 10%
- **TTM覆盖率**: ≥ 80%

### 审计框架
- **自动审计**: 每次数据处理都执行审计检查
- **审计类别**: 数据质量、Schema合规、PIT合规、业务逻辑
- **审计级别**: INFO、WARNING、ERROR、CRITICAL
- **审计报告**: JSON格式，包含详细检查结果

### 决策记录
所有数据处理决策都记录在JSONL文件中：
```json
{
  "action": "deduplicate",
  "strategy": "keep_official_report", 
  "primary_keys": ["symbol", "period_end", "report_type"],
  "original_rows": 1000,
  "final_rows": 980,
  "removed_rows": 20,
  "timestamp": "2024-01-01T10:00:00"
}
```

## 使用指南

### 1. 初始化架构
```python
from src.caiyuangungun.base import DataLayerFactory, DataLayer
from src.caiyuangungun.contracts import DataContract

# 创建数据契约
contract = DataContract()

# 创建各层管理器
raw_manager = DataLayerFactory.create_manager(DataLayer.RAW, "data", contract)
norm_manager = DataLayerFactory.create_manager(DataLayer.NORM, "data", contract)
domain_manager = DataLayerFactory.create_manager(DataLayer.DOMAIN, "data", contract)
qlib_manager = DataLayerFactory.create_manager(DataLayer.QLIB_FEED, "data", contract)
```

### 2. Norm层数据处理
```python
from src.caiyuangungun.norm_layer import NormLayerProcessor
from src.caiyuangungun.contracts import InterfaceType

# 创建处理器
processor = NormLayerProcessor(norm_manager)

# 规范化数据
normalized_data, decisions = processor.normalize_interface_data(
    raw_data, InterfaceType.QUOTES_DAILY
)

# 保存数据
norm_manager.save_data(normalized_data, InterfaceType.QUOTES_DAILY, decisions=decisions)
```

### 3. 数据审计
```python
from src.caiyuangungun.audit import AuditEngine

# 创建审计引擎
audit_engine = AuditEngine(contract)

# 执行审计
results = audit_engine.audit_data(data, InterfaceType.QUOTES_DAILY)

# 保存审计报告
audit_engine.save_audit_report(results, "audit_report.json")
```

## 性能优化

### 存储优化
- **Parquet压缩**: Snappy算法
- **行组大小**: 50,000行
- **分区策略**: 按年度/季度分区
- **列式存储**: 优化查询性能

### 内存管理
- **批处理**: 大数据集分批处理
- **懒加载**: 按需加载数据
- **缓存策略**: 热点数据内存缓存

### 并行处理
- **多进程**: CPU密集型任务
- **异步IO**: 文件读写操作
- **批量操作**: 减少磁盘访问次数

## 扩展性设计

### 新增数据源
1. 在`InterfaceType`枚举中添加新类型
2. 在`InterfaceSchema`中定义字段映射
3. 更新主键定义和去重策略
4. 添加相应的审计规则

### 自定义处理逻辑
1. 继承`BaseDataManager`实现自定义管理器
2. 继承审计检查器添加自定义规则
3. 扩展`DataContract`添加新的配置项

### 多数据源支持
- 每个数据源独立的Raw子目录
- 统一的Norm层标准化处理
- Domain层按数据域合并不同来源

## 最佳实践

### 1. 数据处理流程
```
Raw数据接收 → Norm层规范化 → 质量审计 → Domain层推导 → Qlib投喂
     ↓             ↓            ↓           ↓           ↓
   元数据记录    决策记录      审计报告    业务逻辑    性能优化
```

### 2. 错误处理
- **数据校验**: 在每层入口进行校验
- **异常捕获**: 记录详细错误信息
- **回滚机制**: 支持数据处理回滚
- **告警机制**: 关键错误及时通知

### 3. 监控指标
- **数据完整性**: 监控缺失数据
- **处理性能**: 监控处理时间
- **质量指标**: 监控审计通过率
- **存储使用**: 监控磁盘空间

### 4. 文档维护
- **Schema文档**: 及时更新字段定义
- **变更日志**: 记录架构变更历史
- **运维手册**: 详细的操作说明
- **故障手册**: 常见问题解决方案

## 开发计划

### Phase 1: Norm层开发 (当前)
- [x] 基础架构搭建
- [x] 契约定义
- [x] 审计框架
- [ ] Norm层处理器完善
- [ ] 单元测试

### Phase 2: Domain层开发
- [ ] 财务数据推导算法
- [ ] PIT对齐实现
- [ ] 前向填充逻辑
- [ ] 性能优化

### Phase 3: Qlib集成
- [ ] Qlib投喂格式适配
- [ ] dump_bin流程
- [ ] 特征工程集成
- [ ] 回测验证

### Phase 4: 生产部署
- [ ] 调度系统集成
- [ ] 监控告警
- [ ] 自动化运维
- [ ] 文档完善
