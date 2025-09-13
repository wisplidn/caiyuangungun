# Raw Data Service CLI

原始数据服务的命令行接口，提供统一的数据获取、更新和管理功能。

## 功能特性

- **历史数据回填**: 根据配置的start_date作为起点，自动跳过已存在的数据文件
- **标准数据更新**: 仅更新最新的一份数据，不跳过已存在文件
- **数据更新含回溯**: 最新数据 + lookback_periods 的数据份数
- **指定期间数据获取**: 获取指定时间范围的数据
- **服务状态查询**: 显示数据源、数据定义等服务状态信息
- **数据源管理**: 列出所有可用的数据源及其配置信息

## 安装和使用

### 基本使用

```bash
# 设置环境变量
export PYTHONPATH=/path/to/caiyuangungun/src

# 查看帮助
python -m caiyuangungun.data.raw.cli.run_cli --help

# 查看服务状态
python -m caiyuangungun.data.raw.cli.run_cli status

# 列出所有数据源
python -m caiyuangungun.data.raw.cli.run_cli list
```

### 命令详解

#### 1. 服务状态查询

```bash
# 显示服务状态
python -m caiyuangungun.data.raw.cli.run_cli status
```

#### 2. 数据源管理

```bash
# 列出所有数据源
python -m caiyuangungun.data.raw.cli.run_cli list
```

#### 3. 单个数据获取

```bash
# 获取指定数据源和数据类型的数据
python -m caiyuangungun.data.raw.cli.run_cli fetch -s tushare -d stock_basic

# 获取带日期参数的数据
python -m caiyuangungun.data.raw.cli.run_cli fetch -s tushare -d daily --date-param 20241201

# 强制获取（不跳过已存在文件）
python -m caiyuangungun.data.raw.cli.run_cli fetch -s tushare -d stock_basic --no-skip-existing
```

#### 4. 标准数据更新

```bash
# 更新所有数据
python -m caiyuangungun.data.raw.cli.run_cli update

# 更新指定数据源
python -m caiyuangungun.data.raw.cli.run_cli update --source tushare

# 更新指定存储类型
python -m caiyuangungun.data.raw.cli.run_cli update --storage-type SNAPSHOT

# 更新指定数据类型
python -m caiyuangungun.data.raw.cli.run_cli update --source tushare --data-type stock_basic
```

#### 5. 历史数据回填

```bash
# 回填所有历史数据
python -m caiyuangungun.data.raw.cli.run_cli backfill

# 回填指定数据源的历史数据
python -m caiyuangungun.data.raw.cli.run_cli backfill --source tushare

# 回填指定存储类型的历史数据
python -m caiyuangungun.data.raw.cli.run_cli backfill --storage-type DAILY
```

#### 6. 数据更新含回溯

```bash
# 标准回溯更新（1倍）
python -m caiyuangungun.data.raw.cli.run_cli update-lookback --source tushare

# 自定义回溯倍数
python -m caiyuangungun.data.raw.cli.run_cli update-lookback --source tushare --multiplier 2

# 三倍回溯更新
python -m caiyuangungun.data.raw.cli.run_cli update-triple --source tushare
```

#### 7. 指定期间数据获取

```bash
# 获取指定日期范围的数据
python -m caiyuangungun.data.raw.cli.run_cli fetch-period -s 20241201 -e 20241210

# 获取指定月份的数据
python -m caiyuangungun.data.raw.cli.run_cli fetch-period -s 202412 -e 202412 --storage-type MONTHLY

# 获取指定数据源和类型的期间数据
python -m caiyuangungun.data.raw.cli.run_cli fetch-period -s 20241201 -e 20241210 --source tushare --data-type daily
```

## 参数说明

### 通用参数

- `--source, -s`: 数据源名称（如：tushare）
- `--storage-type, -t`: 存储类型（SNAPSHOT, DAILY, MONTHLY）
- `--data-type, -d`: 数据类型（如：stock_basic, daily, income_q）

### 特殊参数

- `--date-param`: 日期参数（格式：YYYYMMDD 或 YYYYMM）
- `--skip-existing/--no-skip-existing`: 是否跳过已存在文件（默认跳过）
- `--multiplier, -m`: 回溯倍数（用于update-lookback命令）
- `--start-date, -s`: 开始日期（用于fetch-period命令）
- `--end-date, -e`: 结束日期（用于fetch-period命令）

## 输出格式

### 成功示例

```
开始标准数据更新...

处理结果:
  成功: 8
  跳过: 2
  错误: 0
  总计: 10
```

### 错误示例

```
开始获取数据: tushare.invalid_type ...
✗ 错误: 未找到数据定义: tushare.invalid_type
```

## 日志输出

CLI工具会显示详细的日志信息，包括：
- 服务初始化状态
- 数据获取进度
- 错误和警告信息
- 处理结果统计

## 故障排除

### 常见问题

1. **ImportError**: 确保设置了正确的PYTHONPATH
2. **数据源连接失败**: 检查网络连接和API配置
3. **文件权限错误**: 确保有足够的文件系统权限
4. **数据库连接失败**: 检查数据库配置和连接

### 调试模式

可以通过设置环境变量来启用详细日志：

```bash
export PYTHONPATH=/path/to/caiyuangungun/src
export LOG_LEVEL=DEBUG
python -m caiyuangungun.data.raw.cli.run_cli status
```

## 开发和扩展

### 文件结构

```
cli/
├── __init__.py          # 模块初始化和命令注册
├── main.py              # CLI主入口点
├── commands.py          # 所有CLI命令实现
├── run_cli.py           # 便捷运行脚本
└── README.md            # 使用文档
```

### 添加新命令

1. 在`commands.py`中添加新的命令函数
2. 在`main.py`中注册新命令
3. 在`__init__.py`中更新命令注册表

### 自定义配置

可以通过修改配置文件来自定义CLI行为：
- 数据源配置
- 日志级别
- 输出格式
- 默认参数