#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
便捷的CLI启动脚本
可以直接运行此脚本来使用命令行工具
"""

import sys
from pathlib import Path

# 添加当前目录到Python路径
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

if __name__ == '__main__':
    from main import main
    sys.exit(main())