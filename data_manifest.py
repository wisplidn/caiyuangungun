#!/usr/bin/env python3
"""
数据资产清单 (Data Asset Manifest)

该文件是自动化数据管道的核心配置。
它定义了所有需要管理的数据资产、它们的类型、以及如何对它们进行历史回填和日常更新。
"""

# 定义不同更新频率的策略
# 'frequency' 只是一个描述性字段，真正的逻辑由 lookback 参数驱动
UPDATE_POLICIES = {
    'quarterly': {
        'frequency': 'quarterly',
        'lookback_months': 8,  # 回溯8个月，确保能覆盖并修正任何重述的财报
        'run_window': None  # 默认所有月份都运行
    },
    'monthly': {
        'frequency': 'monthly',
        'lookback_months': 12 # 回溯12个月
    },
    'daily_30d_lookback': {
        'frequency': 'daily',
        'lookback_days': 30  # 回溯30天，用于修正日度数据
    },
    'daily_full_reload': {
        'frequency': 'daily',
        'lookback_days': 0   # 适用于按代码全量更新的场景，无需回溯
    },
    'snapshot': {
        'frequency': 'daily' # 快照总是获取最新的
    }
}

# 数据资产清单
DATA_ASSETS = [
    # --- 1. 季度财报数据 (PeriodArchiver) ---
    {'name': 'income', 'archiver': 'period', 'policy': UPDATE_POLICIES['quarterly'], 'backfill_start': '20070101'},
    {'name': 'balancesheet', 'archiver': 'period', 'policy': UPDATE_POLICIES['quarterly'], 'backfill_start': '20070101'},
    {'name': 'cashflow', 'archiver': 'period', 'policy': UPDATE_POLICIES['quarterly'], 'backfill_start': '20070101'},
    {'name': 'fina_indicator', 'archiver': 'period', 'policy': UPDATE_POLICIES['quarterly'], 'backfill_start': '20070101'},
    {'name': 'express', 'archiver': 'period', 'policy': UPDATE_POLICIES['quarterly'], 'backfill_start': '20070101'},
    {'name': 'forecast', 'archiver': 'period', 'policy': UPDATE_POLICIES['quarterly'], 'backfill_start': '20070101'},
    {'name': 'fina_mainbz', 'archiver': 'period', 'policy': UPDATE_POLICIES['quarterly'], 'backfill_start': '20070101'},

    # --- 2. 事件驱动型数据 (DateArchiver) ---
    {'name': 'dividend', 'archiver': 'date', 'policy': UPDATE_POLICIES['daily_30d_lookback'], 'backfill_start': '20070101'},

    # --- 3. 交易日数据 (TradeDateArchiver) ---
    {'name': 'daily', 'archiver': 'trade_date', 'policy': UPDATE_POLICIES['daily_30d_lookback'], 'backfill_start': '19901219'},
    {'name': 'daily_basic', 'archiver': 'trade_date', 'policy': UPDATE_POLICIES['daily_30d_lookback'], 'backfill_start': '20070101'},
    {'name': 'adj_factor', 'archiver': 'trade_date', 'policy': UPDATE_POLICIES['daily_30d_lookback'], 'backfill_start': '20070101'},

    # --- 4. 快照数据 (SnapshotArchiver) ---
    {'name': 'stock_basic', 'archiver': 'snapshot', 'policy': UPDATE_POLICIES['snapshot'], 'backfill_start': None},
    {'name': 'index_basic', 'archiver': 'snapshot', 'policy': UPDATE_POLICIES['snapshot'], 'backfill_start': None},
    {'name': 'index_classify', 'archiver': 'snapshot', 'policy': UPDATE_POLICIES['snapshot'], 'backfill_start': None},
    {'name': 'trade_cal', 'archiver': 'snapshot', 'policy': UPDATE_POLICIES['snapshot'], 'backfill_start': None},

    # --- 5. 代码驱动型数据 (StockDrivenArchiver) ---
    {'name': 'index_daily', 'archiver': 'stock_driven', 'driver_source': 'COMMON_INDEXES', 'policy': UPDATE_POLICIES['daily_full_reload'], 'backfill_start': None},
    {'name': 'stk_holdernumber', 'archiver': 'stock_driven', 'driver_source': 'stock_basic', 'policy': UPDATE_POLICIES['daily_full_reload'], 'backfill_start': None},

    # --- 6. 指数月度数据 (IndexMonthlyArchiver) ---
    {'name': 'index_weight', 'archiver': 'index_monthly', 'policy': UPDATE_POLICIES['monthly'], 'backfill_start': '20070101'},
]

