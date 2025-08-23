"""
配置文件 - Income数据ETL系统
"""

import os
from pathlib import Path

# 基础路径配置
BASE_DATA_PATH = os.getenv('DATA_PATH', './data')

# Tushare配置
TUSHARE_TOKEN = 'q90f4bdab293fe80b426887ee2afa2d3182'
MAX_REQUESTS_PER_MINUTE = 150

# 数据存储路径
RAW_LANDING_PATH = Path(BASE_DATA_PATH) / "raw" / "landing" / "tushare"
RAW_NORM_PATH = Path(BASE_DATA_PATH) / "raw" / "norm" / "tushare"
LOG_PATH = Path(BASE_DATA_PATH) / "logs"

# ETL配置
HISTORICAL_START_YEAR = 2006
HISTORICAL_START_QUARTER = 1
DEFAULT_LOOKBACK_QUARTERS = 12

# 文件格式配置
PARQUET_COMPRESSION = 'snappy'
METADATA_FORMAT = 'json'

# 披露窗口配置（提高检查频率的时期）
DISCLOSURE_PERIODS = {
    'Q1': {'start_month': 4, 'end_month': 4, 'lookback_quarters': 16},
    'Q2': {'start_month': 7, 'end_month': 8, 'lookback_quarters': 16},
    'Q3': {'start_month': 10, 'end_month': 10, 'lookback_quarters': 16},
    'Q4': {'start_month': 1, 'end_month': 4, 'lookback_quarters': 24}
}

# 快照数据配置（用于stock_basic等日频全量更新的数据）
SNAPSHOT_DATA_TYPES = {
    'stock_basic': {
        'description': '股票基础信息',
        'update_frequency': 'daily',
        'retention_days': 30,  # 保留最近30天的快照
        'fields': 'ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs,act_name,act_ent_type'
    }
}
