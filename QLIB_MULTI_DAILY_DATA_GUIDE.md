# Qlib多日频数据源支持指南

## 📋 问题背景

未来可能有多种日频数据需要导入Qlib：
- 行情数据（daily, adj_factor, daily_basic）
- ST标记数据（st_daily）
- 分析师跟踪数据（analyst_coverage）
- 停复牌数据（suspend_daily）
- 等等...

如果设计不当，会导致：
1. ❌ 文件夹冲突（所有数据写入同一目录）
2. ❌ 配置耦合（hard-code数据源）
3. ❌ 难以扩展（新增数据源需要改代码）

## ✅ 解决方案：增强的DailyQlibConverter

### 核心改进

1. **支持多数据源配置**：自动遍历`source_data`中的所有项
2. **避免文件夹冲突**：通过`output_subdir`指定独立输出目录
3. **灵活合并策略**：基于ts_code+trade_date自动合并

### 配置格式

```json
{
  "数据源名称_to_qlib": {
    "description": "描述",
    "output_subdir": "子目录名（用于区分不同数据源）",
    
    "source_data": {
      "数据源1": "路径1",
      "数据源2": "路径2",
      ...
    },
    
    "conversion_config": {
      "required_fields": [],
      "optional_fields": []
    },
    
    "feed_layer": {
      "output_dir": "data/qlib-feed"
    },
    
    "qlib_layer": {
      "output_dir": "data/qlib-data"
    },
    
    "dump_config": {...}
  }
}
```

---

## 📝 配置示例

### 示例1：行情数据（现有）

```json
{
  "daily_quotes_to_qlib": {
    "description": "日频行情数据转Qlib格式",
    "output_subdir": "daily_quotes",
    
    "source_data": {
      "daily": "data/norm/daily_data/cleaned/daily.parquet",
      "adj_factor": "data/norm/daily_data/cleaned/adj_factor.parquet",
      "daily_basic": "data/norm/daily_data/cleaned/daily_basic.parquet"
    },
    
    "conversion_config": {
      "required_fields": ["open", "high", "low", "volume"],
      "adj_factor_field": "factor",
      "basic_fields": ["turnover_rate", "pe", "pb", "ps"]
    },
    
    "feed_layer": {
      "output_dir": "data/qlib-feed"
    },
    
    "qlib_layer": {
      "output_dir": "data/qlib-data"
    },
    
    "dump_config": {
      "script_path": "qlib_repo/scripts/dump_bin.py",
      "date_field_name": "date",
      "file_suffix": ".csv",
      "freq": "day",
      "max_workers": 16
    }
  }
}
```

**目录结构**：
```
data/qlib-feed/daily_quotes/
  ├── SZ000001.csv
  ├── SZ000002.csv
  └── metadata/
      ├── day.txt
      └── all.txt

data/qlib-data/features/
  ├── sz000001/
  │   ├── open.day.bin
  │   ├── high.day.bin
  │   ├── factor.day.bin
  │   └── pe.day.bin
  └── sz000002/
```

---

### 示例2：ST标记数据

```json
{
  "st_daily_to_qlib": {
    "description": "每日ST标记数据转Qlib格式",
    "output_subdir": "daily_st",
    
    "source_data": {
      "st_status": "data/norm/daily_data/cleaned/st_daily.parquet"
    },
    
    "conversion_config": {
      "required_fields": ["is_st", "st_type", "st_start_date"],
      "optional_fields": []
    },
    
    "feed_layer": {
      "output_dir": "data/qlib-feed"
    },
    
    "qlib_layer": {
      "output_dir": "data/qlib-data"
    },
    
    "dump_config": {
      "script_path": "qlib_repo/scripts/dump_bin.py",
      "date_field_name": "date",
      "file_suffix": ".csv",
      "freq": "day",
      "max_workers": 16
    }
  }
}
```

**数据格式要求**：
- 必须包含：`ts_code`, `trade_date`
- 业务字段：`is_st`, `st_type`, `st_start_date`

**目录结构**：
```
data/qlib-feed/daily_st/        ← 独立目录，不冲突
  ├── SZ000001.csv
  └── metadata/

data/qlib-data/features/
  └── sz000001/
      ├── is_st.day.bin        ← 新增字段
      ├── st_type.day.bin      ← 新增字段
      └── open.day.bin         ← 已有字段
```

---

### 示例3：分析师跟踪数据

```json
{
  "analyst_coverage_to_qlib": {
    "description": "分析师跟踪数据转Qlib格式",
    "output_subdir": "daily_analyst",
    
    "source_data": {
      "analyst_coverage": "data/norm/analyst/cleaned/analyst_coverage.parquet"
    },
    
    "conversion_config": {
      "required_fields": ["analyst_count", "rating_avg", "target_price_avg"],
      "optional_fields": ["report_count"]
    },
    
    "feed_layer": {
      "output_dir": "data/qlib-feed"
    },
    
    "qlib_layer": {
      "output_dir": "data/qlib-data"
    },
    
    "dump_config": {
      "script_path": "qlib_repo/scripts/dump_bin.py",
      "date_field_name": "date",
      "file_suffix": ".csv",
      "freq": "day",
      "max_workers": 16
    }
  }
}
```

---

### 示例4：组合多个数据源

```json
{
  "daily_extended_to_qlib": {
    "description": "扩展日频数据（行情+ST+停复牌）",
    "output_subdir": "daily_extended",
    
    "source_data": {
      "daily": "data/norm/daily_data/cleaned/daily.parquet",
      "adj_factor": "data/norm/daily_data/cleaned/adj_factor.parquet",
      "st_status": "data/norm/daily_data/cleaned/st_daily.parquet",
      "suspend_status": "data/norm/daily_data/cleaned/suspend_daily.parquet"
    },
    
    "conversion_config": {
      "required_fields": ["open", "high", "low", "volume", "factor"],
      "optional_fields": ["is_st", "is_suspended"]
    },
    
    "feed_layer": {
      "output_dir": "data/qlib-feed"
    },
    
    "qlib_layer": {
      "output_dir": "data/qlib-data"
    },
    
    "dump_config": {
      "script_path": "qlib_repo/scripts/dump_bin.py",
      "date_field_name": "date",
      "file_suffix": ".csv",
      "freq": "day",
      "max_workers": 16
    }
  }
}
```

---

## 🚀 使用方式

### 方式1：单个数据源转换

```python
from services.qlib_conversion_service import QlibConversionService

service = QlibConversionService()

# 转换ST数据
service.register_from_config('st_daily_to_qlib', 'daily', limit_symbols=100)
result = service.run_converter('st_daily_to_qlib', validate=True)

# 转换分析师数据
service.register_from_config('analyst_coverage_to_qlib', 'daily', limit_symbols=100)
result = service.run_converter('analyst_coverage_to_qlib', validate=True)
```

### 方式2：批量转换所有日频数据

```python
service = QlibConversionService()

# 注册所有日频数据源
daily_converters = [
    'daily_quotes_to_qlib',
    'st_daily_to_qlib',
    'analyst_coverage_to_qlib'
]

for converter_name in daily_converters:
    service.register_from_config(converter_name, 'daily', limit_symbols=100)

# 批量执行
results = service.run_all_converters(validate=True)

# 生成报告
service.generate_report(results, 'daily_data_report.md')
```

### 方式3：在Service层添加便捷函数

```python
# qlib_conversion_service.py 中添加
def create_all_daily_converter_service(config_manager: ConfigManager = None,
                                       limit_symbols: Optional[int] = None) -> QlibConversionService:
    """创建所有日频数据转换服务"""
    service = QlibConversionService(config_manager)
    
    # 获取所有以 daily 相关的配置
    all_configs = service.config_manager.get_all_qlib_conversion_configs()
    daily_configs = [k for k in all_configs.keys() 
                    if 'daily' in k and k.endswith('_to_qlib')]
    
    for config_key in daily_configs:
        service.register_from_config(config_key, 'daily', limit_symbols)
    
    return service
```

---

## 📊 数据流图

```
┌─────────────────────────────────────────────────────────────┐
│                      NORM层（Parquet）                        │
├─────────────────────────────────────────────────────────────┤
│ daily.parquet                                                │
│ adj_factor.parquet                                           │
│ st_daily.parquet                                             │
│ analyst_coverage.parquet                                     │
└────────────┬────────────────────────────────────────────────┘
             │
             ├─ DailyQlibConverter (daily_quotes_to_qlib)
             │  ├─ 读取: daily + adj_factor + daily_basic
             │  ├─ 合并: ts_code + trade_date
             │  └─ 输出: data/qlib-feed/daily_quotes/*.csv
             │
             ├─ DailyQlibConverter (st_daily_to_qlib)
             │  ├─ 读取: st_daily
             │  └─ 输出: data/qlib-feed/daily_st/*.csv
             │
             ├─ DailyQlibConverter (analyst_coverage_to_qlib)
             │  ├─ 读取: analyst_coverage
             │  └─ 输出: data/qlib-feed/daily_analyst/*.csv
             │
             ↓
┌─────────────────────────────────────────────────────────────┐
│                  FEED层（CSV，按股票拆分）                     │
├─────────────────────────────────────────────────────────────┤
│ data/qlib-feed/                                              │
│   ├── daily_quotes/  SZ000001.csv, SZ000002.csv...         │
│   ├── daily_st/      SZ000001.csv, SZ000002.csv...         │
│   └── daily_analyst/ SZ000001.csv, SZ000002.csv...         │
└────────────┬────────────────────────────────────────────────┘
             │
             ├─ dump_bin.py (daily_quotes)
             ├─ dump_bin.py (daily_st)
             ├─ dump_bin.py (daily_analyst)
             │
             ↓
┌─────────────────────────────────────────────────────────────┐
│              QLIB层（Binary，所有字段合并）                     │
├─────────────────────────────────────────────────────────────┤
│ data/qlib-data/features/                                     │
│   └── sz000001/                                              │
│       ├── open.day.bin         ← 来自 daily_quotes          │
│       ├── factor.day.bin       ← 来自 daily_quotes          │
│       ├── is_st.day.bin        ← 来自 daily_st              │
│       └── analyst_count.day.bin ← 来自 daily_analyst         │
└─────────────────────────────────────────────────────────────┘
```

---

## ⚠️ 注意事项

### 1. 数据格式要求
所有日频数据必须包含：
- `ts_code`：股票代码
- `trade_date`：交易日期（YYYYMMDD格式）

### 2. 字段命名冲突
如果多个数据源有同名字段，后加载的会覆盖先加载的。建议：
- 使用明确的字段前缀（如 `st_`, `analyst_`）
- 或在配置中通过field_mapping重命名

### 3. metadata只需一份
- 交易日历和股票列表由第一个日频转换器生成
- 后续转换器会复用（自动覆盖，内容相同）
- 建议先运行 `daily_quotes_to_qlib`

### 4. 执行顺序
```python
# 推荐执行顺序
service.run_converter('daily_quotes_to_qlib')      # 1. 先运行行情数据（生成metadata）
service.run_converter('st_daily_to_qlib')          # 2. 运行其他日频数据
service.run_converter('analyst_coverage_to_qlib')  # 3. ...
```

---

## ✅ 优势总结

1. **无文件夹冲突**：每个数据源独立的`output_subdir`
2. **配置即扩展**：新增数据只需添加配置，无需改代码
3. **灵活组合**：可以一次导入单个数据源，也可以组合多个
4. **自动合并**：Qlib会自动合并所有二进制文件到同一个features目录
5. **向后兼容**：现有配置仍然有效（默认使用'daily'子目录）

---

## 🎯 快速开始

1. **准备数据**：确保数据包含 `ts_code` 和 `trade_date`
2. **添加配置**：在 `qlib_conversion_pipeline_config.json` 中添加配置节
3. **运行转换**：
   ```python
   service = QlibConversionService()
   service.register_from_config('你的配置名_to_qlib', 'daily', limit_symbols=10)
   result = service.run_converter('你的配置名_to_qlib', validate=True)
   ```
4. **验证数据**：使用Qlib读取数据确认成功

搞定！🎉

