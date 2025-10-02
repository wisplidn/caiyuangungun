#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日频数据Qlib转换器

负责将日频行情数据转换为Qlib二进制格式：
- 合并daily、adj_factor、daily_basic数据
- 转换为Qlib格式
- 按股票拆分为CSV文件
- 调用dump_bin.py生成二进制文件
"""

import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd

from ..core.base_qlib_converter import BaseQlibConverter

logger = logging.getLogger('qlib_converter')


class DailyQlibConverter(BaseQlibConverter):
    """
    日频行情数据转换器
    
    专用功能：
    - 加载并合并多个日频数据源
    - 转换股票代码和日期格式
    - 按股票拆分保存CSV文件
    - 调用dump_bin.py生成.bin二进制文件
    """
    
    def __init__(self, config: Dict[str, Any], limit_symbols: Optional[int] = None):
        """
        初始化日频转换器
        
        Args:
            config: 完整的转换器配置字典
            limit_symbols: 限制处理的股票数量
        """
        super().__init__(config, limit_symbols)
        
        # 日频数据特有配置
        self.daily_sources = self.source_data_config
        
        # 支持自定义输出子目录（避免文件夹冲突）
        self.output_subdir = config.get('output_subdir', 'daily')
        
        logger.info(f"DailyQlibConverter 初始化完成 (输出目录: {self.output_subdir})")
    
    def load_source_data(self) -> pd.DataFrame:
        """
        加载并合并日频数据源（支持多数据源配置）
        
        Returns:
            合并后的DataFrame
        """
        logger.info("="*60)
        logger.info("加载并合并日频数据")
        logger.info("="*60)
        
        all_dataframes = []
        selected_symbols = None
        
        # 遍历所有配置的数据源
        for idx, (source_name, source_path) in enumerate(self.daily_sources.items(), 1):
            logger.info(f"步骤{idx}: 读取 {source_name}...")
            
            # 使用通用方法加载（自动处理limit_symbols）
            df = self._load_parquet_with_limit(source_path, symbol_col='ts_code')
            
            if df is None:
                continue
            
            # 第一个数据源确定股票范围
            if idx == 1 and selected_symbols is None and self.limit_symbols:
                selected_symbols = sorted(df['ts_code'].unique())
            elif selected_symbols is not None:
                # 后续数据源使用相同的股票列表
                df = df[df['ts_code'].isin(selected_symbols)]
                logger.info(f"  过滤为 {len(selected_symbols)} 支股票后: {len(df):,} 行")
            
            all_dataframes.append((source_name, df))
        
        if not all_dataframes:
            raise ValueError("没有成功加载任何数据源")
        
        # 合并所有数据源
        logger.info(f"步骤{len(all_dataframes)+1}: 合并所有数据源...")
        
        # 以第一个数据源为基础
        base_name, base_df = all_dataframes[0]
        result_df = base_df.copy()
        logger.info(f"  基础数据: {base_name} ({len(result_df):,} 行)")
        
        # 依次合并其他数据源
        merge_keys = ['ts_code', 'trade_date']
        for source_name, df in all_dataframes[1:]:
            # 获取要合并的列（排除合并键）
            merge_cols = [c for c in df.columns if c not in merge_keys]
            
            result_df = result_df.merge(
                df[merge_keys + merge_cols],
                on=merge_keys,
                how='left'
            )
            logger.info(f"  合并 {source_name} 后: {len(result_df):,} 行")
        
        logger.info(f"✅ 数据合并完成: {len(result_df):,} 行 × {len(result_df.columns)} 列")
        
        return result_df
    
    def convert_to_qlib_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        转换为Qlib格式
        
        Args:
            df: 合并后的源数据DataFrame
            
        Returns:
            Qlib格式DataFrame
        """
        logger.info("="*60)
        logger.info("转换为Qlib格式")
        logger.info("="*60)
        
        # 1. 转换股票代码（使用向量化方法，性能提升100倍以上）
        logger.info("步骤1: 转换股票代码...")
        df['symbol'] = self.convert_symbol_vectorized(df['ts_code'])
        
        # 2. 转换日期格式（优化：避免astype(str)，直接处理整数）
        logger.info("步骤2: 转换日期格式...")
        df['date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        
        # 3. 选择需要的字段
        logger.info("步骤3: 选择字段...")
        
        # 基础字段
        base_fields = ['symbol', 'date']
        
        # 从配置读取字段列表
        required_fields = self.conversion_config.get('required_fields', [])
        optional_fields = self.conversion_config.get('optional_fields', [])
        adj_factor_field = self.conversion_config.get('adj_factor_field', 'factor')
        basic_fields = self.conversion_config.get('basic_fields', [])
        
        # 组合所有字段
        all_fields = base_fields + required_fields + optional_fields + [adj_factor_field] + basic_fields
        
        # 只保留存在的字段
        available_fields = [f for f in all_fields if f in df.columns]
        missing_fields = set(all_fields) - set(available_fields)
        
        logger.info(f"  可用字段: {len(available_fields)} / {len(all_fields)}")
        if missing_fields:
            logger.warning(f"  缺失字段: {missing_fields}")
        
        df_qlib = df[available_fields].copy()
        
        # 4. 排序
        logger.info("步骤4: 排序数据...")
        df_qlib = df_qlib.sort_values(['symbol', 'date'])
        df_qlib = df_qlib.reset_index(drop=True)
        
        logger.info(f"✅ 转换完成: {len(df_qlib):,} 行")
        logger.info(f"   字段列表: {list(df_qlib.columns)}")
        
        return df_qlib
    
    def save_feed_layer(self, df: pd.DataFrame, **kwargs):
        """
        保存FEED层数据（按股票拆分CSV）
        
        Args:
            df: Qlib格式DataFrame
        """
        logger.info("="*60)
        logger.info("保存FEED层数据（按股票拆分CSV）")
        logger.info("="*60)
        
        # 使用配置的子目录，避免不同数据源冲突
        base_output_dir = self.project_root / self.feed_layer_config['output_dir']
        output_dir = base_output_dir / self.output_subdir
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"输出目录: {output_dir}")
        
        # 1. 生成辅助数据
        logger.info("生成辅助数据...")
        calendar_df = self.generate_calendar(df, 'date')
        instruments_df = self.generate_instruments(df, 'symbol', 'date')
        
        # 2. 保存元数据
        self.save_metadata(calendar_df, instruments_df, output_dir)
        
        # 3. 按股票拆分保存CSV（使用通用方法）
        logger.info("步骤3: 按股票拆分保存CSV文件...")
        num_files = self._save_symbols_to_csv(
            df=df,
            output_dir=output_dir,
            file_name_upper=True,  # 文件名大写
            progress_desc="保存股票数据"
        )
        
        logger.info(f"  ✅ 已保存 {num_files} 个CSV文件")
        logger.info(f"     总记录数: {len(df):,}")
        
        logger.info(f"✅ FEED层数据已保存到: {output_dir}")
        logger.info(f"   字段列表: {', '.join([c for c in df.columns if c not in ['symbol', 'date']])}")
    
    def dump_to_qlib_binary(self):
        """
        调用dump_bin.py转换为二进制格式
        """
        logger.info("="*60)
        logger.info("转换为Qlib二进制格式")
        logger.info("="*60)
        
        dump_script = self.project_root / self.dump_config['script_path']
        if not dump_script.exists():
            raise FileNotFoundError(f"dump_bin.py脚本不存在: {dump_script}")
        
        # CSV目录（使用子目录）
        base_csv_dir = self.project_root / self.feed_layer_config['output_dir']
        csv_dir = base_csv_dir / self.output_subdir
        qlib_dir = Path(self.qlib_layer_config['output_dir']).expanduser()
        
        # 确定要导出的字段
        csv_files = list(csv_dir.glob('*.csv'))
        if not csv_files:
            raise FileNotFoundError(f"没有找到CSV文件: {csv_dir}")
        
        # 读取第一个CSV获取列名
        sample_df = pd.read_csv(csv_files[0])
        include_fields = [c for c in sample_df.columns if c not in ['date']]
        include_fields_str = ','.join(include_fields)
        
        logger.info(f"发现 {len(csv_files)} 个CSV文件")
        logger.info(f"包含字段: {include_fields_str}")
        
        # 构建命令
        cmd = [
            sys.executable,
            str(dump_script),
            'dump_all',
            '--data_path', str(csv_dir),
            '--qlib_dir', str(qlib_dir),
            '--date_field_name', self.dump_config.get('date_field_name', 'date'),
            '--file_suffix', self.dump_config.get('file_suffix', '.csv'),
            '--freq', self.dump_config.get('freq', 'day'),
            '--max_workers', str(self.dump_config.get('max_workers', 16)),
            '--include_fields', include_fields_str
        ]
        
        logger.info(f"数据目录: {csv_dir}")
        logger.info(f"Qlib目录: {qlib_dir}")
        
        # 执行命令（使用通用方法）
        self._execute_subprocess(cmd, description="执行dump_bin.py")
        
        # 手动复制元数据文件到qlib目录
        logger.info("复制元数据文件...")
        import shutil
        
        # 复制交易日历
        calendar_src = csv_dir / 'metadata' / 'day.txt'
        calendar_dst = qlib_dir / 'calendars'
        calendar_dst.mkdir(parents=True, exist_ok=True)
        if calendar_src.exists():
            shutil.copy2(calendar_src, calendar_dst / 'day.txt')
            logger.info(f"  ✅ 交易日历: {calendar_dst / 'day.txt'}")
        
        # 复制股票列表
        instruments_src = csv_dir / 'metadata' / 'all.txt'
        instruments_dst = qlib_dir / 'instruments'
        instruments_dst.mkdir(parents=True, exist_ok=True)
        if instruments_src.exists():
            shutil.copy2(instruments_src, instruments_dst / 'all.txt')
            logger.info(f"  ✅ 股票列表: {instruments_dst / 'all.txt'}")
        
        logger.info(f"✅ 二进制数据已保存到: {qlib_dir}")
    
    def validate(self, sample_size: int = 3):
        """
        验证生成的Qlib数据（自动随机抽样）
        
        Args:
            sample_size: 随机抽样股票数量
        """
        logger.info("="*60)
        logger.info("验证Qlib数据（随机抽样）")
        logger.info("="*60)
        
        # 使用通用方法初始化Qlib
        qlib, D = self._init_qlib()
        if qlib is None:
            return
        
        import random
        qlib_dir = Path(self.qlib_layer_config['output_dir']).expanduser()
        
        # 读取转换的股票列表
        base_csv_dir = self.project_root / self.feed_layer_config['output_dir']
        csv_dir = base_csv_dir / self.output_subdir
        instruments_file = csv_dir / 'metadata' / 'all.txt'
        
        if not instruments_file.exists():
            logger.warning(f"❌ 股票列表文件不存在: {instruments_file}")
            return
        
        # 读取所有股票
        import pandas as pd
        instruments_df = pd.read_csv(instruments_file, sep='\t', header=None, 
                                     names=['symbol', 'start_date', 'end_date'])
        all_symbols = instruments_df['symbol'].tolist()
        
        # 随机抽样
        sample_symbols = random.sample(all_symbols, min(sample_size, len(all_symbols)))
        logger.info(f"从 {len(all_symbols)} 支股票中随机抽取 {len(sample_symbols)} 支进行验证")
        logger.info(f"抽样股票: {sample_symbols}")
        
        # 读取第一个CSV文件确定字段
        csv_files = list(csv_dir.glob('*.csv'))
        if csv_files:
            sample_df = pd.read_csv(csv_files[0])
            available_fields = [f'${f}' for f in sample_df.columns if f != 'date']
            test_fields = available_fields[:5]  # 取前5个字段
            logger.info(f"测试字段: {test_fields}")
        else:
            logger.warning("❌ 未找到CSV文件")
            return
        
        # 自动确定日期范围（最近3年）
        from datetime import datetime, timedelta
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=3*365)).strftime('%Y-%m-%d')
        logger.info(f"测试日期: {start_date} 至 {end_date}")
        
        # 验证每只股票
        for symbol in sample_symbols:
            try:
                df = D.features(
                    instruments=[symbol],
                    fields=test_fields,
                    start_time=start_date,
                    end_time=end_date
                )
                
                if df is None or df.empty:
                    logger.warning(f"  ⚠️  {symbol}: 无数据")
                else:
                    logger.info(f"  ✅ {symbol}: {len(df)} 条记录")
                    # 检查空值
                    null_counts = df.isnull().sum()
                    if null_counts.sum() > 0:
                        logger.info(f"      空值统计: {null_counts[null_counts > 0].to_dict()}")
                    
                    # 显示数据预览
                    if len(df) > 0:
                        logger.info(f"      数据预览（前3行）:\n{df.head(3)}")
            
            except Exception as e:
                logger.warning(f"  ⚠️  {symbol} 验证失败: {e}")
        
        logger.info("="*60)
        logger.info("✅ 验证完成")
        logger.info("="*60)

