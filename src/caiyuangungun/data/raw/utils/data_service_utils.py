"""数据服务工具模块

提供数据服务中常用的工具函数，包括：
- 日期处理和生成
- 参数构建和验证
- 数据定义处理
- 统一的遍历逻辑
"""

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Dict, Any, List, Optional, Tuple, Union
import pandas as pd
from enum import Enum

# 导入交易日历功能
try:
    from .trading_calendar import get_latest_trading_day as _get_latest_trading_day
    from .trading_calendar import filter_trading_days as _filter_trading_days
    from .trading_calendar import get_trading_days as _get_trading_days
    
    def get_latest_trading_day(before_date=None, exchange="SSE"):
        """获取最新交易日，带异常处理"""
        try:
            return _get_latest_trading_day(before_date=before_date, exchange=exchange)
        except Exception as e:
            print(f"交易日历功能异常，使用备用方案: {e}")
            if before_date is None:
                before_date = datetime.now()
            elif isinstance(before_date, str):
                before_date = datetime.strptime(before_date, '%Y%m%d')
            yesterday = before_date - timedelta(days=1)
            return yesterday.strftime('%Y%m%d')
    
    def filter_trading_days(date_list, exchange="SSE"):
        """过滤交易日，带异常处理"""
        try:
            return _filter_trading_days(date_list, exchange=exchange)
        except Exception as e:
            print(f"交易日历功能异常，使用备用方案: {e}")
            return date_list
    
    def get_trading_days(start_date=None, end_date=None, exchange="SSE", format_output="%Y%m%d"):
        """获取交易日范围，带异常处理"""
        try:
            return _get_trading_days(start_date=start_date, end_date=end_date, exchange=exchange, format_output=format_output)
        except Exception as e:
            print(f"交易日历功能异常，使用备用方案: {e}")
            if not start_date or not end_date:
                return []
            start = datetime.strptime(start_date, '%Y%m%d')
            end = datetime.strptime(end_date, '%Y%m%d')
            dates = []
            current = start
            while current <= end:
                dates.append(current.strftime(format_output))
                current += timedelta(days=1)
            return dates
            
except ImportError:
    # 如果交易日历模块不可用，提供备用函数
    def get_latest_trading_day(before_date=None, exchange="SSE"):
        """备用函数：返回简单的昨天日期"""
        if before_date is None:
            before_date = datetime.now()
        elif isinstance(before_date, str):
            before_date = datetime.strptime(before_date, '%Y%m%d')
        yesterday = before_date - timedelta(days=1)
        return yesterday.strftime('%Y%m%d')
    
    def filter_trading_days(date_list, exchange="SSE"):
        """备用函数：返回原始日期列表"""
        return date_list
    
    def get_trading_days(start_date=None, end_date=None, exchange="SSE", format_output="%Y%m%d"):
        """备用函数：生成连续日期"""
        if not start_date or not end_date:
            return []
        start = datetime.strptime(start_date, '%Y%m%d')
        end = datetime.strptime(end_date, '%Y%m%d')
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime(format_output))
            current += timedelta(days=1)
        return dates


class RunMode(Enum):
    """运行模式枚举"""
    HISTORICAL_BACKFILL = "historical_backfill"  # 历史数据回填
    STANDARD_UPDATE = "standard_update"  # 标准数据更新
    UPDATE_WITH_LOOKBACK = "update_with_lookback"  # 数据更新含回溯
    UPDATE_WITH_TRIPLE_LOOKBACK = "update_with_triple_lookback"  # 数据更新含回溯_三倍
    FETCH_PERIOD_DATA = "fetch_period_data"  # 指定期间的数据获取


class DateUtils:
    """日期处理工具类"""
    
    @staticmethod
    def get_latest_date(storage_type: str, use_trading_calendar: bool = True) -> str:
        """获取最新日期
        
        Args:
            storage_type: 存储类型 (SNAPSHOT, DAILY, MONTHLY, QUARTERLY)
            use_trading_calendar: 是否使用交易日历（仅对DAILY有效）
            
        Returns:
            最新日期字符串
        """
        now = datetime.now()
        
        if storage_type == 'DAILY':
            if use_trading_calendar:
                # 日频数据：使用交易日历获取最新交易日
                latest_trading_day = get_latest_trading_day(before_date=now)
                return latest_trading_day if latest_trading_day else (now - timedelta(days=1)).strftime('%Y%m%d')
            else:
                # 日频数据：返回昨天的日期（考虑到数据延迟）
                yesterday = now - timedelta(days=1)
                return yesterday.strftime('%Y%m%d')
        elif storage_type == 'MONTHLY':
            # 月频数据：返回上个月
            last_month = now - relativedelta(months=1)
            return last_month.strftime('%Y%m')
        elif storage_type == 'QUARTERLY':
            # 季度数据：返回上个季度末
            current_quarter = (now.month - 1) // 3 + 1
            if current_quarter == 1:
                # 当前是Q1，返回去年Q4
                last_quarter_end = datetime(now.year - 1, 12, 31)
            else:
                # 返回上个季度末
                last_quarter_month = (current_quarter - 1) * 3
                last_quarter_end = datetime(now.year, last_quarter_month, 1) + relativedelta(months=1) - timedelta(days=1)
            return last_quarter_end.strftime('%Y%m%d')
        else:
            # SNAPSHOT数据不需要日期
            return ''
    
    @staticmethod
    def generate_date_range(start_date: str, end_date: str, storage_type: str, 
                          use_trading_calendar: bool = False) -> List[str]:
        """生成日期范围
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            storage_type: 存储类型
            use_trading_calendar: 是否使用交易日历过滤（仅对DAILY有效）
            
        Returns:
            日期字符串列表
        """
        if storage_type == 'SNAPSHOT':
            return []
        
        dates = []
        
        if storage_type == 'DAILY':
            if use_trading_calendar:
                # 使用交易日历直接获取交易日
                dates = get_trading_days(start_date=start_date, end_date=end_date)
            else:
                # 生成连续日期
                start = datetime.strptime(start_date, '%Y%m%d')
                end = datetime.strptime(end_date, '%Y%m%d')
                
                current = start
                while current <= end:
                    dates.append(current.strftime('%Y%m%d'))
                    current += timedelta(days=1)
                
        elif storage_type == 'MONTHLY':
            start = datetime.strptime(start_date, '%Y%m')
            end = datetime.strptime(end_date, '%Y%m')
            
            current = start
            while current <= end:
                dates.append(current.strftime('%Y%m'))
                current += relativedelta(months=1)
                
        elif storage_type == 'QUARTERLY':
            # 季度数据：生成季度末日期
            start = datetime.strptime(start_date, '%Y%m%d')
            end = datetime.strptime(end_date, '%Y%m%d')
            
            # 找到开始日期所在季度的季度末
            start_quarter = (start.month - 1) // 3 + 1
            start_quarter_end = datetime(start.year, start_quarter * 3, 1) + relativedelta(months=1) - timedelta(days=1)
            
            current = start_quarter_end
            while current <= end:
                dates.append(current.strftime('%Y%m%d'))
                # 移动到下一个季度末
                current += relativedelta(months=3)
                # 调整到季度末
                current = datetime(current.year, current.month, 1) + relativedelta(months=1) - timedelta(days=1)
        
        return dates
    
    @staticmethod
    def calculate_lookback_dates(latest_date: str, lookback_periods: int, 
                               storage_type: str, multiplier: int = 1,
                               use_trading_calendar: bool = False) -> List[str]:
        """计算回溯日期
        
        Args:
            latest_date: 最新日期
            lookback_periods: 回溯期数
            storage_type: 存储类型
            multiplier: 倍数
            use_trading_calendar: 是否使用交易日历（仅对DAILY有效）
            
        Returns:
            回溯日期列表
        """
        if storage_type == 'SNAPSHOT' or not latest_date:
            return []
        
        dates = []
        total_periods = lookback_periods * multiplier
        
        if storage_type == 'DAILY':
            if use_trading_calendar:
                # 使用交易日历计算回溯交易日
                # 先生成一个较大的日期范围，然后过滤交易日
                latest = datetime.strptime(latest_date, '%Y%m%d')
                # 估算需要的日期范围（考虑周末和节假日，乘以1.5倍）
                estimated_days = int(total_periods * 1.5)
                start_date = latest - timedelta(days=estimated_days)
                
                # 生成日期范围并过滤交易日
                all_dates = []
                current = start_date
                while current <= latest:
                    all_dates.append(current.strftime('%Y%m%d'))
                    current += timedelta(days=1)
                
                trading_dates = filter_trading_days(all_dates)
                # 取最新的total_periods个交易日
                dates = trading_dates[-total_periods:] if len(trading_dates) >= total_periods else trading_dates
                dates.reverse()  # 从最新到最旧排序
            else:
                # 传统方式：连续日期回溯
                latest = datetime.strptime(latest_date, '%Y%m%d')
                for i in range(total_periods):
                    date = latest - timedelta(days=i)
                    dates.append(date.strftime('%Y%m%d'))
                
        elif storage_type == 'MONTHLY':
            latest = datetime.strptime(latest_date, '%Y%m')
            for i in range(total_periods):
                date = latest - relativedelta(months=i)
                dates.append(date.strftime('%Y%m'))
                
        elif storage_type == 'QUARTERLY':
            latest = datetime.strptime(latest_date, '%Y%m%d')
            for i in range(total_periods):
                # 回溯i个季度
                date = latest - relativedelta(months=i*3)
                # 调整到该季度末
                quarter = (date.month - 1) // 3 + 1
                quarter_end = datetime(date.year, quarter * 3, 1) + relativedelta(months=1) - timedelta(days=1)
                dates.append(quarter_end.strftime('%Y%m%d'))
        
        return dates


class ParameterBuilder:
    """参数构建器"""
    
    @staticmethod
    def build_fetch_params(data_type: str, date_param: Optional[str] = None, 
                          required_params: Optional[Union[List[str], Dict[str, Any]]] = None) -> Dict[str, Any]:
        """构建数据获取参数
        
        Args:
            data_type: 数据类型
            date_param: 日期参数
            required_params: 必需参数配置
            
        Returns:
            构建好的参数字典
        """
        fetch_params = {'data_type': data_type}
        
        # 处理required_params - 方案1：默认值优先，特殊标记使用生成器
        if required_params:
            if isinstance(required_params, dict):
                # required_params是字典，包含默认值或特殊标记
                for param_name, param_value in required_params.items():
                    if param_value == "<TRADE_DATE>":
                        # 特殊标记，使用日期生成器
                        if date_param:
                            fetch_params['trade_date'] = date_param
                    elif param_value == "<MONTHLY_DATE>":
                        # 特殊标记，使用月度日期生成器
                        if date_param:
                            fetch_params['monthly_date'] = date_param
                    elif param_value == "<QUARTERLY_DATE>":
                        # 特殊标记，使用季度日期生成器
                        if date_param:
                            fetch_params[param_name] = date_param
                    else:
                        # 有默认值，直接使用
                        fetch_params[param_name] = param_value
            elif isinstance(required_params, list):
                # required_params是列表，需要根据date_param添加参数（保持向后兼容）
                if date_param:
                    if 'trade_date' in required_params:
                        fetch_params['trade_date'] = date_param
                    elif 'monthly_date' in required_params:
                        fetch_params['monthly_date'] = date_param
                    elif 'quarterly_date' in required_params:
                        fetch_params['quarterly_date'] = date_param
        
        return fetch_params
    
    @staticmethod
    def build_archive_params(source_name: str, data_type: str, data: pd.DataFrame,
                           date_param: Optional[str] = None, 
                           archive_type: Optional[str] = None,
                           constructed_params: Optional[Dict[str, Any]] = None,
                           api_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """构建归档参数
        
        Args:
            source_name: 数据源名称
            data_type: 数据类型
            data: 数据
            date_param: 日期参数
            archive_type: 归档类型
            
        Returns:
            构建好的归档参数字典
        """
        archive_kwargs = {
            'data': data,
            'data_type': data_type,
            'archive_type': archive_type
        }
        
        # 根据归档类型添加对应的日期参数
        if date_param and archive_type:
            if archive_type.upper() == 'DAILY':
                archive_kwargs['daily_date'] = date_param
            elif archive_type.upper() == 'MONTHLY':
                archive_kwargs['monthly_date'] = date_param
            elif archive_type.upper() == 'QUARTERLY':
                archive_kwargs['quarterly_date'] = date_param
            # 保持向后兼容，同时传递date_param
            archive_kwargs['date_param'] = date_param
        
        # 添加参数信息
        if constructed_params is not None:
            archive_kwargs['constructed_params'] = constructed_params
        if api_params is not None:
            archive_kwargs['api_params'] = api_params
        
        return archive_kwargs


class DataDefinitionProcessor:
    """数据定义处理器"""
    
    @staticmethod
    def get_filtered_definitions(data_sources_config: Dict[str, Any],
                               source_name: Optional[str] = None,
                               storage_type: Optional[str] = None,
                               data_type: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """获取过滤后的数据定义
        
        Args:
            data_sources_config: 数据源配置
            source_name: 数据源名称过滤
            storage_type: 存储类型过滤
            data_type: 数据类型过滤
            
        Returns:
            过滤后的数据定义字典
        """
        result = {}
        
        # 如果指定了具体的数据类型和数据源
        if data_type and source_name:
            src_config = data_sources_config.get(source_name, {})
            data_definitions = src_config.get('data_definitions', {})
            definition = data_definitions.get(data_type)
            if definition:
                definition_with_source = definition.copy()
                definition_with_source['source_name'] = source_name
                result[f"{source_name}_{data_type}"] = definition_with_source
            return result
        
        # 遍历所有数据源
        for src_name, src_config in data_sources_config.items():
            # 如果指定了数据源名称，则过滤
            if source_name and src_name != source_name:
                continue
                
            data_definitions = src_config.get('data_definitions', {})
            for dt, definition in data_definitions.items():
                # 如果指定了存储类型，则过滤
                if storage_type and definition.get('storage_type') != storage_type:
                    continue
                
                # 添加源信息
                definition_with_source = definition.copy()
                definition_with_source['source_name'] = src_name
                result[f"{src_name}_{dt}"] = definition_with_source
        
        return result
    
    @staticmethod
    def generate_task_list(definitions: Dict[str, Dict[str, Any]], 
                          run_mode: RunMode,
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None,
                          multiplier: int = 1) -> List[Dict[str, Any]]:
        """根据数据定义和运行模式生成任务列表
        
        Args:
            definitions: 数据定义字典
            run_mode: 运行模式
            start_date: 开始日期（用于指定期间获取）
            end_date: 结束日期（用于指定期间获取）
            multiplier: 回溯倍数
            
        Returns:
            任务列表
        """
        tasks = []
        
        for key, definition in definitions.items():
            src_name = definition['source_name']
            dt = key.split('_', 1)[1]  # 移除source_name前缀
            st = definition.get('storage_type')
            
            base_task = {
                'source_name': src_name,
                'data_type': dt,
                'storage_type': st,
                'definition': definition
            }
            
            if st == 'SNAPSHOT':
                # 快照数据处理
                task = base_task.copy()
                task['date_param'] = None
                task['skip_existing'] = run_mode == RunMode.HISTORICAL_BACKFILL
                tasks.append(task)
                
            elif st in ['DAILY', 'MONTHLY', 'QUARTERLY']:
                # 时间序列数据处理
                date_list = DataDefinitionProcessor._get_date_list_for_mode(
                    definition, run_mode, start_date, end_date, multiplier
                )
                
                for date_str in date_list:
                    task = base_task.copy()
                    task['date_param'] = date_str
                    task['skip_existing'] = run_mode == RunMode.HISTORICAL_BACKFILL
                    tasks.append(task)
        
        return tasks
    
    @staticmethod
    def _get_date_list_for_mode(definition: Dict[str, Any], 
                               run_mode: RunMode,
                               start_date: Optional[str] = None,
                               end_date: Optional[str] = None,
                               multiplier: int = 1) -> List[str]:
        """根据运行模式获取日期列表"""
        st = definition.get('storage_type')
        
        # 检查是否启用交易日历（仅对DAILY数据有效）
        use_trading_calendar = definition.get('use_trading_calendar', True) and st == 'DAILY'
        
        if run_mode == RunMode.HISTORICAL_BACKFILL:
            # 历史回填：从配置的start_date到最新日期
            config_start_date = definition.get('start_date')
            if config_start_date:
                latest_date = DateUtils.get_latest_date(st, use_trading_calendar)
                return DateUtils.generate_date_range(config_start_date, latest_date, st, use_trading_calendar)
            return []
            
        elif run_mode == RunMode.STANDARD_UPDATE:
            # 标准更新：只获取最新日期
            latest_date = DateUtils.get_latest_date(st, use_trading_calendar)
            return [latest_date] if latest_date else []
            
        elif run_mode in [RunMode.UPDATE_WITH_LOOKBACK, RunMode.UPDATE_WITH_TRIPLE_LOOKBACK]:
            # 回溯更新：最新日期 + 回溯期数
            latest_date = DateUtils.get_latest_date(st, use_trading_calendar)
            lookback_periods = definition.get('lookback_periods', 1)
            if run_mode == RunMode.UPDATE_WITH_TRIPLE_LOOKBACK:
                multiplier = 3
            return DateUtils.calculate_lookback_dates(latest_date, lookback_periods, st, multiplier, use_trading_calendar)
            
        elif run_mode == RunMode.FETCH_PERIOD_DATA:
            # 指定期间：使用提供的日期范围
            if start_date and end_date:
                return DateUtils.generate_date_range(start_date, end_date, st, use_trading_calendar)
            return []
        
        return []


class ResultProcessor:
    """结果处理器"""
    
    @staticmethod
    def create_success_result(source_name: str, data_type: str, date_param: Optional[str],
                            data_shape: List[int], archive_result: Dict[str, Any]) -> Dict[str, Any]:
        """创建成功结果"""
        return {
            'status': 'success',
            'source_name': source_name,
            'data_type': data_type,
            'date_param': date_param,
            'data_shape': data_shape,
            'archive_result': archive_result
        }
    
    @staticmethod
    def create_error_result(source_name: str, data_type: str, date_param: Optional[str],
                          error: str) -> Dict[str, Any]:
        """创建错误结果"""
        return {
            'status': 'error',
            'source_name': source_name,
            'data_type': data_type,
            'date_param': date_param,
            'error': error
        }
    
    @staticmethod
    def create_skipped_result(source_name: str, data_type: str, date_param: Optional[str],
                            reason: str) -> Dict[str, Any]:
        """创建跳过结果"""
        return {
            'status': 'skipped',
            'reason': reason,
            'source_name': source_name,
            'data_type': data_type,
            'date_param': date_param
        }
    
    @staticmethod
    def summarize_results(results: List[Dict[str, Any]]) -> Dict[str, int]:
        """汇总结果统计"""
        summary = {
            'success': 0,
            'error': 0,
            'skipped': 0,
            'validation_failed': 0,
            'total': len(results)
        }
        
        for result in results:
            status = result.get('status', 'unknown')
            if status in summary:
                summary[status] += 1
        
        return summary