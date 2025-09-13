# QLIB-READY 数据层

## 概述

QLIB-READY层是数据处理流水线的最后一层，负责将NORM层的标准化数据转换为符合Qlib格式要求的数据文件。该层采用模块化设计，支持多个数据域的处理，确保数据格式完全兼容Qlib框架。

## 架构设计

```
qlib_ready/
├── core/                    # 核心抽象组件
│   ├── base_processor.py    # 数据处理器基类
│   ├── base_manager.py      # 数据管理器基类
│   └── validator.py         # 格式验证器
├── processors/              # 数据域处理器
│   └── quotes/             # 行情数据处理器
│       ├── processor.py    # 行情数据处理逻辑
│       └── manager.py      # 行情数据管理器
├── cli/                    # 命令行接口
│   ├── manager.py          # CLI管理器
│   ├── quotes_cli.py       # 行情数据CLI
│   └── cli.py             # 传统CLI（向后兼容）
├── examples/               # 使用示例
├── tests/                  # 测试文件
└── __init__.py            # 模块导出
```

## 主要功能

- **多域支持**: 支持行情、财务、分析师等多个数据域
- **数据提取**: 从NORM层提取各类标准化数据
- **数据连接**: 与复权因子和基础信息进行左连接
- **数据清洗**: 处理异常值和缺失数据
- **格式转换**: 转换为Qlib标准格式
- **特征分组**: 按特征类型分组存储
- **质量验证**: 验证数据格式和完整性
- **扩展性**: 易于添加新的数据域处理器

## 核心组件

### 基础抽象类

#### BaseQlibProcessor
所有数据域处理器的基类，定义了统一的处理接口:
- `process_data()`: 数据处理主流程
- `feature_mapping`: 特征映射配置
- `required_columns`: 必需列定义
- 通用的数据清洗和验证方法

#### BaseQlibManager
所有数据域管理器的基类，提供:
- 数据路径管理
- 存储和读取接口
- 元数据管理
- 质量检查功能

### 行情数据处理

#### QlibReadyDataManager

行情数据管理器类，负责整体的数据管理和存储。

```python
from src.caiyuangungun.data.qlib_ready.manager import QlibReadyDataManager
from src.caiyuangungun.contracts import InterfaceType

# 初始化管理器
manager = QlibReadyDataManager(data_root="/path/to/data")

# 生成Qlib格式数据
result = manager.generate_qlib_data(
    start_date=date(2024, 1, 1),
    end_date=date(2024, 12, 31),
    symbols=['000001.SZ', '000002.SZ']
)

# 保存特定特征数据
file_path = manager.save_data(
    data=feature_data,
    interface_type=InterfaceType.QUOTES_DAILY,
    feature_name='open'
)
```

#### QlibDataProcessor

行情数据处理器类，实现核心的数据处理逻辑。

```python
from src.caiyuangungun.data.qlib_ready.processors.quotes.processor import QlibDataProcessor

# 初始化处理器
processor = QlibDataProcessor()

# 处理行情数据
processed_data = processor.process_quotes_data(
    daily_quotes=daily_quotes_df,
    adj_factors=adj_factors_df,
    basic_info=basic_info_df
)

# 按特征分组
feature_data = processor.split_by_features(processed_data)

# 数据质量验证
quality_report = processor.validate_data_quality(processed_data)
```

#### QlibFormatValidator

格式验证器类，确保数据符合Qlib要求。

```python
from src.caiyuangungun.data.qlib_ready.core.validator import QlibFormatValidator

# 初始化验证器
validator = QlibFormatValidator()

# 验证数据格式
validation_result = validator.validate_qlib_format(
    data=feature_df,
    feature_name='open'
)

# 生成验证报告
report = validator.generate_validation_report(validation_results)
```

## 使用方式

### 1. 命令行接口（推荐）

#### 新版多域CLI
```bash
# 处理行情数据
python -m src.caiyuangungun.data.qlib_ready.cli.manager quotes process \
    --start-date 2024-01-01 \
    --end-date 2024-12-31 \
    --data-root /path/to/data

# 验证行情数据
python -m src.caiyuangungun.data.qlib_ready.cli.manager quotes validate \
    --symbols 000001.SZ 000002.SZ

# 列出可用股票
python -m src.caiyuangungun.data.qlib_ready.cli.manager quotes list --limit 100

# 清理数据
python -m src.caiyuangungun.data.qlib_ready.cli.manager quotes clean --confirm
```

#### 传统CLI（向后兼容）
```bash
# 通过主项目CLI
python cli.py qlib process --help

# 直接调用
python -m src.caiyuangungun.data.qlib_ready.cli.cli process \
    --start-date 2024-01-01 \
    --end-date 2024-12-31
```

### 2. Python API

#### 使用统一接口
```python
from src.caiyuangungun.data.qlib_ready import (
    QlibReadyDataManager,
    QlibDataProcessor,
    QlibFormatValidator
)
from src.caiyuangungun.contracts import InterfaceType

# 初始化管理器
manager = QlibReadyDataManager('/path/to/data')

# 处理数据
result = manager.process_data(
    interface_type=InterfaceType.QUOTES_DAILY,
    start_date=date(2024, 1, 1),
    end_date=date(2024, 12, 31)
)
```

#### 扩展新的数据域
```python
from src.caiyuangungun.data.qlib_ready.core.base_processor import BaseQlibProcessor
from src.caiyuangungun.data.qlib_ready.core.base_manager import BaseQlibManager

# 创建新的数据域处理器
class FinancialProcessor(BaseQlibProcessor):
    @property
    def feature_mapping(self):
        return {
            'revenue': '$revenue',
            'profit': '$profit'
        }
    
    def process_data(self, data):
        # 实现财务数据处理逻辑
        pass

# 创建新的数据域管理器
class FinancialManager(BaseQlibManager):
    @property
    def supported_interface_types(self):
        return [InterfaceType.FIN_IS, InterfaceType.FIN_BS]
    
    def get_processor(self, interface_type):
        return FinancialProcessor()
```

## 数据格式

### 输入数据格式

#### Daily Quotes
```
symbol | trade_date | open | high | low | close | volume | amount | ...
-------|------------|------|------|-----|-------|--------|--------|----- 
000001.SZ | 2024-01-01 | 10.5 | 10.8 | 10.2 | 10.6 | 1000000 | 10600000 | ...
```

#### Adjustment Factors
```
symbol | trade_date | adj_factor
-------|------------|----------
000001.SZ | 2024-01-01 | 1.0
```

#### Basic Info
```
symbol | name | industry | market | list_date | status
-------|------|----------|--------|-----------|-------
000001.SZ | 平安银行 | 银行 | 主板 | 1991-04-03 | L
```

### 输出数据格式

每个特征保存为独立的CSV文件，格式如下：

```
trade_date,000001.SZ,000002.SZ,600000.SH
2024-01-01,10.5,25.3,15.8
2024-01-02,10.6,25.1,15.9
```

## 支持的特征

- **open**: 开盘价
- **high**: 最高价
- **low**: 最低价
- **close**: 收盘价
- **volume**: 成交量
- **amount**: 成交额
- **factor**: 复权因子
- **vwap**: 成交量加权平均价
- **change**: 涨跌额
- **pct_chg**: 涨跌幅

## 完整使用示例

```python
import pandas as pd
from datetime import date
from src.caiyuangungun.data.qlib_ready.manager import QlibReadyDataManager

def main():
    # 1. 初始化管理器
    data_root = "/path/to/your/data"
    manager = QlibReadyDataManager(data_root)
    
    # 2. 设置参数
    start_date = date(2024, 1, 1)
    end_date = date(2024, 12, 31)
    symbols = ['000001.SZ', '000002.SZ', '600000.SH']
    
    # 3. 生成Qlib数据
    try:
        result = manager.generate_qlib_data(
            start_date=start_date,
            end_date=end_date,
            symbols=symbols
        )
        
        print(f"数据处理完成:")
        print(f"- 处理行数: {result['processed_rows']}")
        print(f"- 生成特征: {len(result['feature_files'])}个")
        print(f"- 保存路径: {result['save_path']}")
        
        # 4. 查看生成的文件
        for feature, file_path in result['feature_files'].items():
            print(f"  {feature}: {file_path}")
            
    except Exception as e:
        print(f"处理失败: {e}")

if __name__ == "__main__":
    main()
```

## 数据质量检查

系统提供完整的数据质量检查功能：

- **基础格式检查**: 数据类型、行列数验证
- **索引格式检查**: 日期列格式、序列完整性
- **列格式检查**: 股票代码有效性、列名唯一性
- **数据类型检查**: 数值列类型、缺失值统计
- **特征名称检查**: 标准特征名称、命名规范
- **数据完整性检查**: 缺失值比例、行完整性
- **数据一致性检查**: 日期唯一性、时间间隔

## 错误处理

系统提供详细的错误信息和警告：

- **数据格式错误**: 不符合Qlib格式要求
- **缺失值警告**: 数据缺失超过阈值
- **异常值检测**: 极值和负值检测
- **日期格式错误**: 日期列格式不正确
- **股票代码错误**: 无效的股票代码格式

## 性能优化

- **批量处理**: 支持大批量数据处理
- **内存优化**: 分块处理大数据集
- **并行处理**: 特征分组并行保存
- **缓存机制**: 重复数据缓存优化

## 注意事项

1. **数据依赖**: 需要NORM层数据作为输入
2. **存储空间**: 按特征分组会增加存储需求
3. **内存使用**: 大数据集处理需要足够内存
4. **日期格式**: 确保日期格式为YYYY-MM-DD
5. **股票代码**: 使用标准的6位代码+后缀格式

## 故障排除

### 常见问题

1. **路径错误**: 确保数据根目录存在且有写权限
2. **数据格式**: 检查输入数据是否符合预期格式
3. **内存不足**: 减少处理批次大小或增加系统内存
4. **依赖缺失**: 确保安装了所有必要的Python包

### 调试建议

1. 启用详细日志记录
2. 使用小数据集进行测试
3. 检查中间处理结果
4. 验证数据质量报告