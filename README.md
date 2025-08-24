# 自动化量化数据管道

这是一个由元数据驱动的、自动化的Tushare数据下载与管理管道。它的核心目标是为量化研究提供一个稳定、可靠、可扩展的数据基础。

## 核心特性

- **元数据驱动**: 所有数据资产的类型、更新策略和历史回填配置都在 `data_manifest.py` 中集中管理，易于扩展。
- **自动化**: 支持全自动的历史数据回填和日常增量更新。
- **数据质量保障**: 内置“校验-重试-报告”工作流，能自动发现并修复常见的数据缺失问题。
- **版本化存储**: 所有数据都以分区形式存储，并对更新进行版本化归档，确保数据的可追溯性。
- **高效稳定**: 智能的更新策略和对API的友好调用，确保了数据获取的高效和稳定。

## 如何开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置 Tushare Token

在项目根目录下创建一个 `.env` 文件，并填入你的Tushare API Token：

```
TUSHARE_TOKEN=your_actual_tushare_token_here
```

### 3. (可选) 自定义数据资产

打开 `data_manifest.py`，你可以根据自己的需求，轻松地添加、删除或修改任何数据资产的配置。

## 核心工作流

项目的主要交互入口是 `pipeline.py`。

### 1. 历史数据回填

该命令将根据 `data_manifest.py` 中的配置，为所有资产下载完整的历史数据。它支持断点续传，可以安全地多次运行。

```bash
python pipeline.py --mode backfill
```

### 2. 日常增量更新

这是最常用的命令。它将为所有资产智能地更新近期数据，并自动运行数据质量检查与修复流程。

```bash
python pipeline.py --mode update
```

### 3. 独立数据质量检查

如果你想在不下载任何数据的情况下，独立地对本地数据进行一次完整的质量检查和自动修复，请运行：

```bash
python pipeline.py --mode quality_check
```

## 手动调试

对于开发者，`main.py` 提供了一个用于手动调试单个数据归档器的工具。这在开发新的数据接口或排查特定问题时非常有用。

**示例**: 对 `income`（利润表）数据，从 `20230101` 开始进行一次手动回填。

```bash
python main.py --archiver-type period --data-type income --mode backfill --start-date 20230101
```







