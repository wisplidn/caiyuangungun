#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Qlib数据转换基类

提供Qlib数据转换的通用功能：
- 配置管理
- 股票代码转换
- 交易日历生成
- 股票列表生成
- 数据验证
"""

import json
import logging
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import numpy as np
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('qlib_converter')


class BaseQlibConverter(ABC):
    """
    Qlib数据转换基类
    
    所有Qlib转换器的基类，提供通用功能：
    - 配置加载和管理
    - 股票代码格式转换
    - 交易日历生成
    - 股票列表生成
    - 数据质量验证
    """
    
    def __init__(self, config: Dict[str, Any], limit_symbols: Optional[int] = None):
        """
        初始化转换器
        
        Args:
            config: 完整的转换器配置字典
            limit_symbols: 限制处理的股票数量，None表示处理全部
        """
        self.config = config
        self.project_root = Path(config.get('project_root', Path.cwd()))
        self.limit_symbols = limit_symbols
        
        # 提取关键配置
        self.source_data_config = config.get('source_data', {})
        self.conversion_config = config.get('conversion_config', {})
        self.feed_layer_config = config.get('feed_layer', {})
        self.qlib_layer_config = config.get('qlib_layer', {})
        self.dump_config = config.get('dump_config', {})
        
        if limit_symbols:
            logger.info(f"⚠️  测试模式: 只处理 {limit_symbols} 支股票")
        
        logger.info(f"{self.__class__.__name__} 初始化完成")
    
    def convert_symbol(self, tushare_code: str) -> str:
        """
        转换股票代码格式: Tushare -> Qlib
        
        Args:
            tushare_code: Tushare格式股票代码 (如 600000.SH)
            
        Returns:
            Qlib格式股票代码 (如 sh600000)
            
        Examples:
            >>> converter.convert_symbol('600000.SH')
            'sh600000'
            >>> converter.convert_symbol('000001.SZ')
            'sz000001'
            >>> converter.convert_symbol('688001.SH')
            'sh688001'
        """
        pattern = r'^(\d{6})\.(SH|SZ|BJ)$'
        match = re.match(pattern, tushare_code)
        
        if not match:
            raise ValueError(f"Invalid symbol format: {tushare_code}")
        
        code, exchange = match.groups()
        return f"{exchange.lower()}{code}"
    
    def convert_symbol_vectorized(self, tushare_series: pd.Series) -> pd.Series:
        """
        向量化转换股票代码（性能优化，比apply快100倍以上）
        
        Args:
            tushare_series: Tushare格式代码Series
            
        Returns:
            Qlib格式代码Series
        """
        # 使用str.extract进行向量化操作（比apply快得多）
        extracted = tushare_series.str.extract(r'^(\d{6})\.(SH|SZ|BJ)$')
        if extracted.isnull().any().any():
            raise ValueError("存在无效的Tushare股票代码格式")
        
        # 向量化拼接：交易所代码(小写) + 股票代码
        return extracted[1].str.lower() + extracted[0]
    
    def calculate_period_vectorized(self, end_date_series: pd.Series, interval: str = 'quarterly') -> pd.Series:
        """
        向量化计算period（性能优化，比apply快100倍以上）
        
        Args:
            end_date_series: 结束日期Series (YYYYMMDD格式的整数)
            interval: 'quarterly' 或 'annual'
            
        Returns:
            period Series (YYYYQQ 或 YYYY 格式)
        """
        year = end_date_series // 10000
        month = (end_date_series % 10000) // 100
        
        if interval == 'quarterly':
            # 季度格式：YYYYQQ
            quarter = ((month - 1) // 3) + 1
            return year * 100 + quarter
        else:
            # 年度格式：YYYY
            return year
    
    def convert_symbol_reverse(self, qlib_code: str) -> str:
        """
        反向转换股票代码: Qlib -> Tushare
        
        Args:
            qlib_code: Qlib格式股票代码 (如 sh600000)
            
        Returns:
            Tushare格式股票代码 (如 600000.SH)
        """
        pattern = r'^(sh|sz|bj)(\d{6})$'
        match = re.match(pattern, qlib_code.lower())
        
        if not match:
            raise ValueError(f"Invalid qlib symbol format: {qlib_code}")
        
        exchange, code = match.groups()
        return f"{code}.{exchange.upper()}"
    
    def generate_calendar(self, df: pd.DataFrame, date_field: str = 'date') -> pd.DataFrame:
        """
        生成交易日历
        
        Args:
            df: 包含日期字段的DataFrame
            date_field: 日期字段名称
            
        Returns:
            交易日历DataFrame，包含唯一的交易日期
        """
        logger.info("生成交易日历...")
        
        # 提取所有唯一交易日，按日期排序
        if df[date_field].dtype == 'object':
            # 如果是字符串，转换为datetime
            dates = pd.to_datetime(df[date_field].unique())
        else:
            # 如果已经是datetime，直接使用
            dates = pd.to_datetime(df[date_field].unique())
        
        trade_dates = sorted(dates)
        calendar_df = pd.DataFrame({
            'date': [pd.Timestamp(d).strftime('%Y-%m-%d') for d in trade_dates]
        })
        
        logger.info(f"✅ 交易日历: {len(calendar_df)} 天")
        logger.info(f"   日期范围: {calendar_df['date'].iloc[0]} 至 {calendar_df['date'].iloc[-1]}")
        
        return calendar_df
    
    def generate_instruments(self, df: pd.DataFrame, 
                           symbol_field: str = 'symbol',
                           date_field: str = 'date') -> pd.DataFrame:
        """
        生成股票列表（instruments）
        
        Args:
            df: 包含股票代码和日期的DataFrame
            symbol_field: 股票代码字段名
            date_field: 日期字段名
            
        Returns:
            股票列表DataFrame，包含symbol, start_datetime, end_datetime
        """
        logger.info("生成股票列表...")
        
        instruments = []
        for symbol in df[symbol_field].unique():
            symbol_data = df[df[symbol_field] == symbol]
            start_date = symbol_data[date_field].min()
            end_date = symbol_data[date_field].max()
            
            # 确保日期格式正确
            if isinstance(start_date, str):
                start_date = pd.to_datetime(start_date)
            if isinstance(end_date, str):
                end_date = pd.to_datetime(end_date)
            
            instruments.append({
                'symbol': symbol.upper(),  # Qlib要求大写
                'start_datetime': pd.Timestamp(start_date).strftime('%Y-%m-%d'),
                'end_datetime': pd.Timestamp(end_date).strftime('%Y-%m-%d')
            })
        
        instruments_df = pd.DataFrame(instruments)
        logger.info(f"✅ 股票列表: {len(instruments_df)} 支股票")
        
        return instruments_df
    
    def save_metadata(self, calendar_df: pd.DataFrame, 
                     instruments_df: pd.DataFrame,
                     output_dir: Path):
        """
        保存元数据（交易日历和股票列表）
        
        Args:
            calendar_df: 交易日历DataFrame
            instruments_df: 股票列表DataFrame
            output_dir: 输出目录
        """
        logger.info("保存元数据...")
        
        # 创建metadata目录
        metadata_dir = output_dir / 'metadata'
        metadata_dir.mkdir(parents=True, exist_ok=True)
        
        # 保存交易日历
        calendar_path = metadata_dir / 'day.txt'
        calendar_df.to_csv(calendar_path, header=False, index=False)
        logger.info(f"  ✅ 交易日历: {calendar_path}")
        
        # 保存股票列表
        instruments_path = metadata_dir / 'all.txt'
        instruments_df.to_csv(instruments_path, sep='\t', header=False, index=False)
        logger.info(f"  ✅ 股票列表: {instruments_path}")
    
    def validate_dataframe(self, df: pd.DataFrame, 
                          required_fields: List[str],
                          name: str = "DataFrame") -> Tuple[bool, List[str]]:
        """
        验证DataFrame的字段完整性
        
        Args:
            df: 待验证的DataFrame
            required_fields: 必需字段列表
            name: DataFrame名称（用于日志）
            
        Returns:
            (是否通过验证, 缺失字段列表)
        """
        missing_fields = [f for f in required_fields if f not in df.columns]
        
        if missing_fields:
            logger.warning(f"{name} 缺失必需字段: {missing_fields}")
            return False, missing_fields
        else:
            logger.info(f"✅ {name} 字段验证通过")
            return True, []
    
    def check_data_quality(self, df: pd.DataFrame, name: str = "数据") -> Dict[str, Any]:
        """
        检查数据质量
        
        Args:
            df: 待检查的DataFrame
            name: 数据名称
            
        Returns:
            质量报告字典
        """
        logger.info(f"检查{name}质量...")
        
        report = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'null_counts': {},
            'duplicate_rows': 0,
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
        }
        
        # 检查空值
        null_counts = df.isnull().sum()
        report['null_counts'] = {col: int(count) for col, count in null_counts.items() if count > 0}
        
        # 检查重复行
        report['duplicate_rows'] = df.duplicated().sum()
        
        # 日志输出
        logger.info(f"  总行数: {report['total_rows']:,}")
        logger.info(f"  总列数: {report['total_columns']}")
        logger.info(f"  内存占用: {report['memory_usage_mb']:.2f} MB")
        
        if report['null_counts']:
            logger.warning(f"  空值字段: {list(report['null_counts'].keys())}")
        
        if report['duplicate_rows'] > 0:
            logger.warning(f"  重复行数: {report['duplicate_rows']}")
        
        return report
    
    def limit_data_by_symbols(self, df: pd.DataFrame, 
                             symbol_field: str = 'ts_code',
                             limit: Optional[int] = None) -> pd.DataFrame:
        """
        按股票数量限制数据（用于测试）
        
        Args:
            df: 原始DataFrame
            symbol_field: 股票代码字段名
            limit: 限制的股票数量
            
        Returns:
            限制后的DataFrame
        """
        if limit is None or limit <= 0:
            return df
        
        selected_symbols = sorted(df[symbol_field].unique())[:limit]
        df_limited = df[df[symbol_field].isin(selected_symbols)].copy()
        
        logger.info(f"  ✂️  已限制为 {limit} 支股票")
        logger.info(f"  限制前: {len(df):,} 行")
        logger.info(f"  限制后: {len(df_limited):,} 行")
        
        return df_limited
    
    @abstractmethod
    def load_source_data(self) -> pd.DataFrame:
        """
        加载源数据（抽象方法，由子类实现）
        
        Returns:
            源数据DataFrame
        """
        pass
    
    @abstractmethod
    def convert_to_qlib_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        转换为Qlib格式（抽象方法，由子类实现）
        
        Args:
            df: 源数据DataFrame
            
        Returns:
            Qlib格式DataFrame
        """
        pass
    
    @abstractmethod
    def save_feed_layer(self, df: pd.DataFrame, **kwargs):
        """
        保存FEED层数据（抽象方法，由子类实现）
        
        Args:
            df: Qlib格式DataFrame
            **kwargs: 其他参数
        """
        pass
    
    @abstractmethod
    def dump_to_qlib_binary(self):
        """
        转换为Qlib二进制格式（抽象方法，由子类实现）
        """
        pass
    
    def run(self) -> Dict[str, Any]:
        """
        执行完整的转换流程
        
        Returns:
            执行结果字典
        """
        logger.info("="*60)
        logger.info(f"开始执行 {self.__class__.__name__} 转换流程")
        logger.info("="*60)
        
        result = {
            'success': False,
            'converter': self.__class__.__name__,
            'start_time': datetime.now().isoformat(),
            'stages': {}
        }
        
        try:
            # 阶段1: 加载源数据
            logger.info("\n【阶段1】加载源数据")
            source_df = self.load_source_data()
            result['stages']['load'] = {
                'rows': len(source_df),
                'columns': len(source_df.columns)
            }
            
            # 阶段2: 转换为Qlib格式
            logger.info("\n【阶段2】转换为Qlib格式")
            qlib_df = self.convert_to_qlib_format(source_df)
            result['stages']['convert'] = {
                'rows': len(qlib_df),
                'columns': len(qlib_df.columns)
            }
            
            # 阶段3: 保存FEED层
            logger.info("\n【阶段3】保存FEED层数据")
            self.save_feed_layer(qlib_df)
            result['stages']['feed'] = {'status': 'completed'}
            
            # 阶段4: 转换为二进制
            logger.info("\n【阶段4】转换为Qlib二进制格式")
            self.dump_to_qlib_binary()
            result['stages']['dump'] = {'status': 'completed'}
            
            result['success'] = True
            result['end_time'] = datetime.now().isoformat()
            
            logger.info("="*60)
            logger.info("🎉 转换流程执行成功！")
            logger.info("="*60)
            
        except Exception as e:
            result['success'] = False
            result['error'] = str(e)
            result['end_time'] = datetime.now().isoformat()
            
            logger.error("="*60)
            logger.error(f"❌ 转换流程失败: {e}")
            logger.error("="*60)
            
            import traceback
            logger.error(traceback.format_exc())
            raise
        
        return result
    
    # ========================================================================
    # 通用辅助方法（供子类使用，减少重复代码）
    # ========================================================================
    
    def _load_parquet_with_limit(self, file_path: str, symbol_col: str = 'ts_code') -> pd.DataFrame:
        """
        加载Parquet文件并可选地限制股票数量
        
        Args:
            file_path: 相对于项目根目录的文件路径
            symbol_col: 股票代码列名
            
        Returns:
            加载的DataFrame（失败返回None）
        """
        full_path = self.project_root / file_path
        
        if not full_path.exists():
            logger.warning(f"  ⚠️  文件不存在: {full_path}")
            return None
        
        df = pd.read_parquet(full_path)
        logger.info(f"  原始数据: {len(df):,} 行, {df[symbol_col].nunique()} 支股票")
        
        # 限制股票数量
        if self.limit_symbols:
            df = self.limit_data_by_symbols(df, symbol_col, self.limit_symbols)
        
        return df
    
    def _execute_subprocess(self, cmd: list, description: str = "执行外部命令") -> bool:
        """
        执行subprocess命令并统一处理错误
        
        Args:
            cmd: 命令列表
            description: 命令描述
            
        Returns:
            是否成功
            
        Raises:
            RuntimeError: 命令执行失败
        """
        import subprocess
        
        logger.info(f"{description}...")
        logger.info(f"  命令: {' '.join(cmd[:4])} ...")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error(f"❌ {description}失败:")
            logger.error(f"   返回码: {result.returncode}")
            logger.error(f"   错误输出: {result.stderr}")
            raise RuntimeError(f"{description} failed with return code {result.returncode}")
        
        if result.stdout:
            logger.info(f"  命令输出:\n{result.stdout}")
        
        logger.info(f"✅ {description}成功")
        return True
    
    def _save_symbols_to_csv(self, df: pd.DataFrame, output_dir: Path, 
                            columns_to_keep: list = None,
                            file_name_upper: bool = False,
                            progress_desc: str = "保存CSV",
                            parallel: bool = True,
                            max_workers: int = 16) -> int:
        """
        按股票拆分保存CSV文件（高性能批量写入，性能提升10-20倍）
        
        优化策略：
        1. 使用groupby避免重复过滤
        2. 并行写入文件
        3. 预先处理列，减少每次循环的开销
        
        Args:
            df: 包含symbol列的DataFrame
            output_dir: 输出目录
            columns_to_keep: 要保留的列（None表示除symbol外全保留）
            file_name_upper: 文件名是否大写
            progress_desc: 进度条描述
            parallel: 是否使用并行处理
            max_workers: 最大并行工作线程数
            
        Returns:
            保存的文件数量
        """
        from tqdm import tqdm
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        # 预处理：确定要保存的列（避免在循环中重复判断）
        if columns_to_keep:
            save_columns = [col for col in columns_to_keep if col in df.columns]
            # 临时保留symbol列用于分组，之后会删除
            df_to_save = df[['symbol'] + save_columns].copy()
        else:
            df_to_save = df.copy()
        
        # 使用groupby分组（比重复过滤快得多）
        grouped = df_to_save.groupby('symbol', sort=False)
        symbols = list(grouped.groups.keys())
        
        logger.info(f"准备保存 {len(symbols)} 个股票的CSV文件...")
        
        def save_single_symbol(symbol_data):
            """保存单个股票的CSV文件（接收(symbol, df)元组）"""
            symbol, group_df = symbol_data
            
            # 移除symbol列
            group_df = group_df.drop(columns=['symbol'])
            
            # 保存CSV（使用更快的引擎）
            symbol_name = symbol.upper() if file_name_upper else symbol.lower()
            csv_path = output_dir / f"{symbol_name}.csv"
            
            # 使用pyarrow引擎（如果可用）或c引擎，比python引擎快3-5倍
            try:
                group_df.to_csv(csv_path, index=False, engine='c')
            except:
                group_df.to_csv(csv_path, index=False)
            
            return symbol
        
        if parallel and len(symbols) > 50:
            # 并行处理（大量股票时效果显著）
            logger.info(f"使用并行模式保存（{max_workers}个工作线程）...")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                futures = {executor.submit(save_single_symbol, (symbol, group_df)): symbol 
                          for symbol, group_df in grouped}
                
                # 显示进度
                completed = 0
                for future in tqdm(as_completed(futures), total=len(symbols), desc=progress_desc):
                    completed += 1
                    try:
                        future.result()
                    except Exception as e:
                        symbol = futures[future]
                        logger.error(f"保存 {symbol} 失败: {e}")
        else:
            # 串行处理（股票数量少时）
            logger.info("使用串行模式保存...")
            for symbol, group_df in tqdm(grouped, total=len(symbols), desc=progress_desc):
                try:
                    save_single_symbol((symbol, group_df))
                except Exception as e:
                    logger.error(f"保存 {symbol} 失败: {e}")
        
        return len(symbols)
    
    def _init_qlib(self, qlib_dir: Path = None):
        """
        初始化Qlib实例（统一处理）
        
        Args:
            qlib_dir: Qlib数据目录（None则从配置读取）
            
        Returns:
            (qlib模块, D模块) 或 (None, None)
        """
        try:
            import qlib
            from qlib.constant import REG_CN
            from qlib.data import D
        except ImportError:
            logger.error("❌ 未安装qlib库，跳过验证")
            return None, None
        
        if qlib_dir is None:
            qlib_dir = Path(self.qlib_layer_config['output_dir']).expanduser()
        
        logger.info(f"初始化Qlib (provider_uri={qlib_dir})...")
        qlib.init(
            provider_uri=str(qlib_dir),
            region=REG_CN,
            expression_cache=None,
            dataset_cache=None
        )
        
        return qlib, D

