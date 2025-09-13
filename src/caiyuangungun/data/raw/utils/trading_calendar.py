#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
交易日历工具模块

提供交易日相关的工具函数，用于限制数据获取的日期范围。
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Union
import logging

logger = logging.getLogger(__name__)

# 交易日历数据路径
TRADE_CAL_PATH = "/Users/daishun/个人文档/caiyuangungun/data/raw/landing/tushare/trade_cal/data.parquet"


def get_trading_days(
    start_date: Union[str, datetime] = None,
    end_date: Union[str, datetime] = None,
    exchange: str = "SSE",
    format_output: str = "%Y%m%d"
) -> List[str]:
    """
    获取指定日期范围内的交易日列表
    
    Args:
        start_date: 开始日期，格式为YYYYMMDD字符串或datetime对象
        end_date: 结束日期，格式为YYYYMMDD字符串或datetime对象
        exchange: 交易所代码，默认为SSE（上海证券交易所）
        format_output: 输出日期格式，默认为%Y%m%d
    
    Returns:
        交易日列表，格式为YYYYMMDD字符串列表
    
    Raises:
        FileNotFoundError: 交易日历文件不存在
        ValueError: 日期格式错误
    """
    try:
        # 读取交易日历数据
        df = pd.read_parquet(TRADE_CAL_PATH)
        
        # 筛选指定交易所的数据
        df = df[df['exchange'] == exchange]
        
        # 筛选交易日（is_open=1）
        trading_days_df = df[df['is_open'] == 1].copy()
        
        # 转换日期格式
        trading_days_df['cal_date'] = pd.to_datetime(trading_days_df['cal_date'], format='%Y%m%d')
        
        # 应用日期范围筛选
        if start_date is not None:
            if isinstance(start_date, str):
                start_date = pd.to_datetime(start_date, format='%Y%m%d')
            elif isinstance(start_date, datetime):
                start_date = pd.to_datetime(start_date)
            trading_days_df = trading_days_df[trading_days_df['cal_date'] >= start_date]
        
        if end_date is not None:
            if isinstance(end_date, str):
                end_date = pd.to_datetime(end_date, format='%Y%m%d')
            elif isinstance(end_date, datetime):
                end_date = pd.to_datetime(end_date)
            trading_days_df = trading_days_df[trading_days_df['cal_date'] <= end_date]
        
        # 排序并格式化输出
        trading_days_df = trading_days_df.sort_values('cal_date')
        trading_days = trading_days_df['cal_date'].dt.strftime(format_output).tolist()
        
        logger.info(f"获取到 {len(trading_days)} 个交易日，日期范围: {start_date} 到 {end_date}")
        
        return trading_days
        
    except FileNotFoundError:
        logger.error(f"交易日历文件不存在: {TRADE_CAL_PATH}")
        raise
    except Exception as e:
        logger.error(f"获取交易日列表时发生错误: {str(e)}")
        raise


def filter_trading_days(
    date_list: List[str],
    exchange: str = "SSE"
) -> List[str]:
    """
    从给定的日期列表中筛选出交易日
    
    Args:
        date_list: 日期列表，格式为YYYYMMDD字符串列表
        exchange: 交易所代码，默认为SSE
    
    Returns:
        筛选后的交易日列表
    """
    try:
        # 读取交易日历数据
        df = pd.read_parquet(TRADE_CAL_PATH)
        
        # 筛选指定交易所的交易日
        trading_days_set = set(
            df[(df['exchange'] == exchange) & (df['is_open'] == 1)]['cal_date'].tolist()
        )
        
        # 筛选交易日
        filtered_dates = [date for date in date_list if date in trading_days_set]
        
        logger.info(f"从 {len(date_list)} 个日期中筛选出 {len(filtered_dates)} 个交易日")
        
        return filtered_dates
        
    except Exception as e:
        logger.error(f"筛选交易日时发生错误: {str(e)}")
        raise


def is_trading_day(
    date: Union[str, datetime],
    exchange: str = "SSE"
) -> bool:
    """
    判断指定日期是否为交易日
    
    Args:
        date: 日期，格式为YYYYMMDD字符串或datetime对象
        exchange: 交易所代码，默认为SSE
    
    Returns:
        是否为交易日
    """
    try:
        # 格式化日期
        if isinstance(date, datetime):
            date_str = date.strftime('%Y%m%d')
        else:
            date_str = str(date)
        
        # 读取交易日历数据
        df = pd.read_parquet(TRADE_CAL_PATH)
        
        # 查询指定日期是否为交易日
        result = df[
            (df['exchange'] == exchange) & 
            (df['cal_date'] == date_str) & 
            (df['is_open'] == 1)
        ]
        
        return len(result) > 0
        
    except Exception as e:
        logger.error(f"判断交易日时发生错误: {str(e)}")
        return False


def get_latest_trading_day(
    before_date: Union[str, datetime] = None,
    exchange: str = "SSE"
) -> Optional[str]:
    """
    获取指定日期之前的最近交易日
    
    Args:
        before_date: 参考日期，默认为当前日期
        exchange: 交易所代码，默认为SSE
    
    Returns:
        最近交易日，格式为YYYYMMDD字符串，如果没有找到则返回None
    """
    try:
        if before_date is None:
            before_date = datetime.now()
        elif isinstance(before_date, str):
            before_date = pd.to_datetime(before_date, format='%Y%m%d')
        
        # 读取交易日历数据
        df = pd.read_parquet(TRADE_CAL_PATH)
        
        # 筛选指定交易所的交易日
        trading_days_df = df[
            (df['exchange'] == exchange) & 
            (df['is_open'] == 1)
        ].copy()
        
        # 转换日期格式并筛选
        trading_days_df['cal_date'] = pd.to_datetime(trading_days_df['cal_date'], format='%Y%m%d')
        trading_days_df = trading_days_df[trading_days_df['cal_date'] < before_date]
        
        if len(trading_days_df) == 0:
            return None
        
        # 获取最近的交易日
        latest_trading_day = trading_days_df['cal_date'].max()
        
        return latest_trading_day.strftime('%Y%m%d')
        
    except Exception as e:
        logger.error(f"获取最近交易日时发生错误: {str(e)}")
        return None