#!/bin/bash
# Raw Data Service CLI Runner
# 便捷的CLI运行脚本
#
# 常用命令示例：
# ./run_cli.sh --help                    # 查看帮助信息
# ./run_cli.sh status                    # 查看服务状态
# ./run_cli.sh list                      # 列出所有数据源
# ./run_cli.sh update                    # 标准数据更新
# ./run_cli.sh update --storage-type SNAPSHOT  # 更新指定存储类型
# ./run_cli.sh update --source tushare   # 更新指定数据源
# ./run_cli.sh backfill                  # 历史数据回填
# ./run_cli.sh fetch -s tushare -d stock_basic  # 获取单个数据
# ./run_cli.sh fetch-period -s 20241201 -e 20241210  # 获取指定期间数据
# ./run_cli.sh update-lookback           # 数据更新含回溯
# ./run_cli.sh update-triple             # 数据更新含三倍回溯

# 设置项目根目录
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 设置PYTHONPATH
export PYTHONPATH="$PROJECT_ROOT/src:$PYTHONPATH"

# 切换到项目目录
cd "$PROJECT_ROOT"

# 运行CLI命令
python -m caiyuangungun.data.raw.cli.main "$@"