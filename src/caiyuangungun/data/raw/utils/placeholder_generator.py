#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PlaceholderGenerator - 占位符生成器

负责根据占位符类型、日期范围和回看参数生成相应的日期列表。
支持的占位符类型：
- <TRADE_DATE>: 交易日期
- <MONTHLY_DATE_RANGE>: 月度日期范围
- <QUARTERLY_DATE>: 季度日期
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Union, Any
from pathlib import Path


class PlaceholderGenerator:
    """占位符生成器
    
    根据占位符类型、日期范围和回看参数生成相应的日期列表
    """
    
    # 支持的占位符类型
    SUPPORTED_PLACEHOLDERS = [
        '<TRADE_DATE>',
        '<MONTHLY_DATE_RANGE>',
        '<QUARTERLY_DATE>',
        '<AK_LISTED_SYMBOL>'
    ]
    
    def __init__(self):
        """初始化占位符生成器"""
        self.trade_cal_path = "/Users/daishun/个人文档/caiyuangungun/data/raw/landing/tushare/trade_cal/data.parquet"
        self._trade_cal_cache = None
    
    def generate_placeholder_values(
        self,
        placeholder: str,
        start_date: str,
        end_date: str,
        lookback_periods: int,
        lookback_multiplier: float = 1.0,
        truncate_mode: bool = True
    ) -> Dict[str, List[str]]:
        """
        生成占位符对应的日期值
        
        Args:
            placeholder: 占位符类型
            start_date: 开始日期 (格式: YYYYMM 或 YYYYMMDD)
            end_date: 结束日期 (格式: YYYYMM 或 YYYYMMDD)
            lookback_periods: 回看期数
            lookback_multiplier: 回看倍数
            truncate_mode: 是否启用截断模式，默认为True
            
        Returns:
            包含占位符和对应日期列表的字典
            
        Raises:
            ValueError: 当占位符类型不支持或日期格式错误时
        """
        # 1. 验证占位符类型
        if placeholder not in self.SUPPORTED_PLACEHOLDERS:
            raise ValueError(f"不支持的占位符类型: {placeholder}. 支持的类型: {self.SUPPORTED_PLACEHOLDERS}")
        
        # 2. 标准化日期格式
        normalized_start, normalized_end = self._normalize_dates(start_date, end_date)
        
        # 3. 根据占位符类型生成日期列表
        if placeholder == '<TRADE_DATE>':
            result = self._generate_trade_dates(normalized_start, normalized_end)
        elif placeholder == '<MONTHLY_DATE_RANGE>':
            result = self._generate_monthly_date_range(normalized_start, normalized_end)
        elif placeholder == '<QUARTERLY_DATE>':
            result = self._generate_quarterly_dates(normalized_start, normalized_end)
        elif placeholder == '<AK_LISTED_SYMBOL>':
            result = self._generate_ak_listed_symbols()
        
        # 4. 应用回看期数限制（仅在截断模式下）
        if truncate_mode and placeholder != '<AK_LISTED_SYMBOL>':
            max_periods = max(1, int(lookback_periods * lookback_multiplier))
            result = self._apply_lookback_limit(result, max_periods)
        
        return result
    
    def process_params_dict(
        self,
        params_dict: Dict[str, Any],
        start_date: str,
        end_date: str,
        lookback_periods: int,
        lookback_multiplier: float = 1.0,
        truncate_mode: bool = True
    ) -> Dict[str, Any]:
        """
        处理参数字典，将占位符替换为对应的值列表
        
        Args:
            params_dict: 参数字典
            start_date: 开始日期
            end_date: 结束日期
            lookback_periods: 回看期数
            lookback_multiplier: 回看倍数
            truncate_mode: 是否启用截断模式
            
        Returns:
            处理后的参数字典
        """
        processed_params = {}
        
        for param_name, param_value in params_dict.items():
            if isinstance(param_value, str) and param_value.startswith('<') and param_value.endswith('>'):
                # 处理占位符
                placeholder = param_value  # 保持完整的占位符格式
                
                try:
                    placeholder_dict = self.generate_placeholder_values(
                        placeholder=placeholder,
                        start_date=start_date,
                        end_date=end_date,
                        lookback_periods=lookback_periods,
                        lookback_multiplier=lookback_multiplier,
                        truncate_mode=truncate_mode
                    )
                    
                    # 特殊处理MONTHLY_DATE_RANGE：直接替换字典的key
                    if placeholder == '<MONTHLY_DATE_RANGE>':
                        processed_params['start_date'] = placeholder_dict.get('start_date', [])
                        processed_params['end_date'] = placeholder_dict.get('end_date', [])
                    else:
                        # 正常占位符：保留原key，替换为列表值
                        if placeholder in placeholder_dict:
                            processed_params[param_name] = placeholder_dict[placeholder]
                        else:
                            processed_params[param_name] = []
                except Exception as e:
                    # 处理失败时保持原值
                    processed_params[param_name] = param_value
            else:
                # 非占位符：保留原key和原值
                processed_params[param_name] = param_value
        
        return processed_params
    
    def _normalize_dates(self, start_date: str, end_date: str) -> tuple:
        """
        标准化日期格式
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            标准化后的日期元组 (start_date, end_date)
            
        Raises:
            ValueError: 当日期格式不正确时
        """
        def normalize_single_date(date_str: str, is_start: bool = True) -> str:
            if len(date_str) == 6:  # YYYYMM格式
                year = int(date_str[:4])
                month = int(date_str[4:6])
                
                if is_start:
                    # 开始日期填充至月初
                    return f"{year:04d}{month:02d}01"
                else:
                    # 结束日期填充至月末
                    if month == 12:
                        next_year = year + 1
                        next_month = 1
                    else:
                        next_year = year
                        next_month = month + 1
                    
                    # 计算月末日期
                    next_month_first = datetime(next_year, next_month, 1)
                    month_end = next_month_first - timedelta(days=1)
                    return month_end.strftime('%Y%m%d')
                    
            elif len(date_str) == 8:  # YYYYMMDD格式
                # 验证日期格式是否正确
                try:
                    datetime.strptime(date_str, '%Y%m%d')
                    return date_str
                except ValueError:
                    raise ValueError(f"无效的日期格式: {date_str}")
            else:
                raise ValueError(f"不支持的日期格式: {date_str}. 支持格式: YYYYMM 或 YYYYMMDD")
        
        normalized_start = normalize_single_date(start_date, is_start=True)
        normalized_end = normalize_single_date(end_date, is_start=False)
        
        return normalized_start, normalized_end
    
    def _load_trade_calendar(self) -> pd.DataFrame:
        """
        加载交易日历数据
        
        Returns:
            交易日历DataFrame
            
        Raises:
            FileNotFoundError: 当交易日历文件不存在时
        """
        if self._trade_cal_cache is None:
            if not os.path.exists(self.trade_cal_path):
                raise FileNotFoundError(f"交易日历文件不存在: {self.trade_cal_path}")
            
            self._trade_cal_cache = pd.read_parquet(self.trade_cal_path)
            # 确保日期列为字符串格式
            if 'cal_date' in self._trade_cal_cache.columns:
                self._trade_cal_cache['cal_date'] = self._trade_cal_cache['cal_date'].astype(str)
        
        return self._trade_cal_cache
    
    def _generate_trade_dates(self, start_date: str, end_date: str) -> Dict[str, List[str]]:
        """
        生成交易日期列表
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            
        Returns:
            包含交易日期列表的字典
        """
        trade_cal = self._load_trade_calendar()
        
        # 筛选交易日期范围
        mask = (
            (trade_cal['cal_date'] >= start_date) & 
            (trade_cal['cal_date'] <= end_date) &
            (trade_cal['is_open'] == 1)  # 只选择开市日期
        )
        
        trade_dates = trade_cal[mask]['cal_date'].tolist()
        trade_dates.sort(reverse=True)  # 按日期由近至远排序
        
        return {'<TRADE_DATE>': trade_dates}
    
    def _generate_monthly_date_range(self, start_date: str, end_date: str) -> Dict[str, List[str]]:
        """
        生成月度日期范围
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            
        Returns:
            包含月初和月末日期列表的字典
        """
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        
        month_starts = []
        month_ends = []
        
        current_dt = start_dt.replace(day=1)  # 从开始月份的月初开始
        
        while current_dt <= end_dt:
            # 月初日期
            month_start = current_dt.strftime('%Y%m%d')
            
            # 月末日期
            if current_dt.month == 12:
                next_month = current_dt.replace(year=current_dt.year + 1, month=1)
            else:
                next_month = current_dt.replace(month=current_dt.month + 1)
            
            month_end = (next_month - timedelta(days=1)).strftime('%Y%m%d')
            
            # 确保不超过结束日期
            if current_dt.strftime('%Y%m%d') <= end_date:
                month_starts.append(month_start)
                month_ends.append(min(month_end, end_date))
            
            # 移动到下一个月
            if current_dt.month == 12:
                current_dt = current_dt.replace(year=current_dt.year + 1, month=1)
            else:
                current_dt = current_dt.replace(month=current_dt.month + 1)
        
        # 按日期由近至远排序
        month_starts.sort(reverse=True)
        month_ends.sort(reverse=True)
        
        return {
            'start_date': month_starts,
            'end_date': month_ends
        }
    
    def _generate_quarterly_dates(self, start_date: str, end_date: str) -> Dict[str, List[str]]:
        """
        生成季度财报日期
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            
        Returns:
            包含季度日期列表的字典
        """
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        
        quarterly_dates = []
        
        # 财报日期通常是每季度末：3月31日、6月30日、9月30日、12月31日
        quarter_end_months = [3, 6, 9, 12]
        quarter_end_days = [31, 30, 30, 31]
        
        # 扩展年份范围，确保能覆盖到相关的季度
        start_year = start_dt.year - 1  # 向前扩展一年
        end_year = end_dt.year       # 向后扩展一年
        
        current_year = start_year
        while current_year <= end_year:
            for i, month in enumerate(quarter_end_months):
                try:
                    quarter_date = datetime(current_year, month, quarter_end_days[i])
                    quarter_date_str = quarter_date.strftime('%Y%m%d')
                    
                    # 修改逻辑：生成所有可能相关的季度日期，不严格限制在范围内
                    # 这样可以确保季度数据的完整性
                    quarterly_dates.append(quarter_date_str)
                        
                except ValueError:
                    # 处理2月29日等特殊情况
                    continue
            
            current_year += 1
        
        # 按日期由近至远排序
        quarterly_dates.sort(reverse=True)
        
        return {'<QUARTERLY_DATE>': quarterly_dates}
    
    def _generate_ak_listed_symbols(self) -> Dict[str, List[str]]:
        """
        生成AK格式的上市股票代码列表
        
        从tushare stock_basic数据中提取上市状态的公司，
        将ts_code转换成AK格式（如SH600519）
        只包含上市状态的股票，剔除退市股票
        
        Returns:
            包含股票代码列表的字典
        """
        try:
            # 读取股票基础信息
            stock_basic_path = "/Users/daishun/个人文档/caiyuangungun/data/raw/landing/tushare/stock_basic/data.parquet"
            df = pd.read_parquet(stock_basic_path)
            
            # 筛选上市状态的股票（list_status='L'表示上市）
            listed_df = df[df['list_status'] == 'L']
            
            # 转换ts_code格式为AK格式
            # 000001.SZ -> SZ000001, 600519.SH -> SH600519
            ak_symbols = []
            for ts_code in listed_df['ts_code'].tolist():
                if '.' in ts_code:
                    code, exchange = ts_code.split('.')
                    ak_symbol = f"{exchange}{code}"
                    ak_symbols.append(ak_symbol)
            
            print(f"成功生成{len(ak_symbols)}个上市股票代码")
            return {'<AK_LISTED_SYMBOL>': ak_symbols}
            
        except Exception as e:
            print(f"生成AK股票代码列表时出错: {e}")
            return {'<AK_LISTED_SYMBOL>': []}
    
    def _apply_lookback_limit(self, result: Dict[str, List[str]], max_periods: int) -> Dict[str, List[str]]:
        """
        应用回看期数限制
        
        Args:
            result: 原始结果字典
            max_periods: 最大期数
            
        Returns:
            截断后的结果字典
        """
        limited_result = {}
        
        for key, date_list in result.items():
            # 截取最近的max_periods个日期
            limited_result[key] = date_list[:max_periods]
        
        return limited_result