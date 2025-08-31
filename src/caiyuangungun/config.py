"""
配置文件 - Income数据ETL系统
"""


# --- Centralized List of Common Indexes ---
# This list is shared by IndexMonthlyArchiver and StockDrivenArchiver (for index_daily)
COMMON_INDEXES = [
    '000001.SH', # 上证指数
    '000300.SH', # 沪深300
    '000905.SH', # 中证500
    '000016.SH', # 上证50
    '399001.SZ', # 深证成指
    '399006.SZ', # 创业板指
    '000688.SH', # 科创50
    '399106.SZ', # 深证综指
    '000852.SH', # 中证1000
    '399295.SZ', # 创成长
    'hsi.hi',    # 恒生指数
    'hstech.hi', # 恒生科技
    '000932.SH', # 中证消费
    '000991.SH', # 全指医药
    '399986.SZ', # CS新能车
    '000922.SH', # 中证红利
    '399971.SZ', # CS传媒
    '000827.SH', # 中证环保
    '000934.SH', # 中证金融
    '000993.SH', # 全指信息
    '399967.SZ', # 中证军工
    '000811.SH', # 全指可选
    '000992.SH', # 全指消费
    '000989.SH', # 全指材料
    '000990.SH', # 全指工业
    '000994.SH', # 全指能源
    '399804.SZ', # CS智汽车
    '931188.CSI',# CS AI
    '930713.CSI',# 中证动漫
    '930647.CSI' # 中证白酒
]

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
INTERIM_PATH = Path(BASE_DATA_PATH) / "interim"  # 中间处理层，与raw、qlib平级
QUOTES_DAY_PATH = INTERIM_PATH / "quotes_day"    # 日频交易数据
LOG_PATH = Path(BASE_DATA_PATH) / "logs"

# ETL配置
HISTORICAL_START_YEAR = 2006
HISTORICAL_START_QUARTER = 1
DEFAULT_LOOKBACK_QUARTERS = 12

# 清洗处理配置
CLEANING_START_DATE = "20170101"  # 清洗处理起始日期
CLEANING_BATCH_SIZE = 100         # 批处理大小（天数）

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
