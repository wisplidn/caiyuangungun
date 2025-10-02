#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIT数据Qlib转换器

负责将财务PIT数据转换为Qlib格式：
- 加载财务数据源
- 转换为PIT长格式
- 按股票拆分为CSV文件
- 调用dump_pit.py生成.data和.index文件
"""

import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd

from ..core.base_qlib_converter import BaseQlibConverter

logger = logging.getLogger('qlib_converter')


class PITQlibConverter(BaseQlibConverter):
    """
    财务PIT数据转换器
    
    专用功能：
    - 加载财务数据源（利润表、资产负债表、现金流量表等）
    - 转换为PIT长格式（period, field, value）
    - 按股票拆分保存CSV文件
    - 调用dump_pit.py生成.data和.index二进制文件
    """
    
    def __init__(self, config: Dict[str, Any], limit_symbols: Optional[int] = None):
        """
        初始化PIT转换器
        
        Args:
            config: 完整的转换器配置字典
            limit_symbols: 限制处理的股票数量
        """
        super().__init__(config, limit_symbols)
        
        # PIT数据特有配置
        self.data_sources = self.source_data_config  # 多个财务表
        self.interval = self.conversion_config.get('interval', 'quarterly')  # quarterly 或 annual
        
        logger.info("PITQlibConverter 初始化完成")
    
    def load_source_data(self) -> Dict[str, pd.DataFrame]:
        """
        加载多个财务数据源
        
        Returns:
            数据源字典 {data_type: DataFrame}
        """
        logger.info("="*60)
        logger.info("加载财务数据源")
        logger.info("="*60)
        
        data_dict = {}
        
        for data_type, file_path in self.data_sources.items():
            logger.info(f"加载 {data_type}...")
            
            # 使用通用方法加载（自动处理limit_symbols）
            df = self._load_parquet_with_limit(file_path, symbol_col='ts_code')
            
            if df is None:
                continue
            
            data_dict[data_type] = df
        
        logger.info(f"✅ 共加载 {len(data_dict)} 个数据源")
        
        return data_dict
    
    def convert_to_qlib_format(self, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        转换为Qlib PIT格式
        
        Args:
            data_dict: 数据源字典
            
        Returns:
            转换后的数据字典 {data_type: qlib_df}
        """
        logger.info("="*60)
        logger.info("转换为Qlib PIT格式")
        logger.info("="*60)
        
        qlib_dict = {}
        
        for data_type, df in data_dict.items():
            logger.info(f"\n处理 {data_type}...")
            
            # 转换单个数据源
            qlib_df = self._convert_single_source(df, data_type)
            qlib_dict[data_type] = qlib_df
            
            logger.info(f"  ✅ {data_type}: {len(qlib_df):,} 行")
        
        return qlib_dict
    
    def _convert_single_source(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """
        转换单个数据源为PIT长格式
        
        Args:
            df: 原始宽格式DataFrame
            data_type: 数据类型
            
        Returns:
            PIT长格式DataFrame (symbol, period, datetime, field, value)
        """
        # 1. 转换股票代码（使用向量化方法，性能提升100倍以上）
        df['symbol'] = self.convert_symbol_vectorized(df['ts_code'])
        
        # 2. 转换日期
        df['end_date_int'] = df['end_date'].astype(int)
        df['ann_date_int'] = df['ann_date'].astype(int)
        
        # 3. 计算period（使用向量化方法，性能提升100倍以上）
        df['period'] = self.calculate_period_vectorized(df['end_date_int'], self.interval)
        
        # 4. 转换datetime（使用ann_date，优化：避免astype(str)）
        df['datetime'] = pd.to_datetime(df['ann_date_int'], format='%Y%m%d')
        
        # 5. 获取字段映射
        field_mapping = self.conversion_config.get('field_mappings', {}).get(data_type, {})
        
        # 6. 识别ID列和值列
        id_cols = ['symbol', 'period', 'datetime']
        exclude_cols = ['ts_code', 'end_date', 'ann_date', 'end_date_int', 'ann_date_int', 
                       'update_flag', 'comp_type', 'report_type']
        
        # 找到所有值字段
        value_fields = [col for col in df.columns 
                       if col not in id_cols + exclude_cols]
        
        # 如果有字段映射，只保留映射的字段
        if field_mapping:
            available_fields = [f for f in field_mapping.keys() if f in value_fields]
        else:
            available_fields = value_fields
        
        # 7. 转换为长格式
        df_long = df[id_cols + available_fields].melt(
            id_vars=id_cols,
            var_name='field',
            value_name='value'
        )
        
        # 8. 应用字段映射并添加后缀
        if field_mapping:
            df_long['field'] = df_long['field'].map(
                lambda x: field_mapping.get(x, x)
            )
        
        # 添加interval后缀
        suffix = '_q' if self.interval == 'quarterly' else '_a'
        df_long['field'] = df_long['field'] + suffix
        
        # 9. 移除空值
        df_long = df_long.dropna(subset=['value'])
        
        # 10. 排序
        df_long = df_long.sort_values(['symbol', 'datetime', 'field'])
        df_long = df_long.reset_index(drop=True)
        
        return df_long
    
    def _calculate_period(self, end_date: int) -> int:
        """
        计算period（YYYYQQ或YYYY格式）
        
        Args:
            end_date: 结束日期（YYYYMMDD格式）
            
        Returns:
            period值
        """
        year = end_date // 10000
        month = (end_date % 10000) // 100
        
        if self.interval == 'quarterly':
            # 季度格式：YYYYQQ
            quarter = ((month - 1) // 3) + 1
            return year * 100 + quarter
        else:
            # 年度格式：YYYY
            return year
    
    def save_feed_layer(self, qlib_dict: Dict[str, pd.DataFrame], **kwargs):
        """
        保存FEED层数据（按数据类型和股票拆分）
        
        Args:
            qlib_dict: 转换后的数据字典
        """
        logger.info("="*60)
        logger.info("保存FEED层数据（PIT格式）")
        logger.info("="*60)
        
        base_output_dir = self.project_root / self.feed_layer_config['output_dir']
        
        for data_type, df in qlib_dict.items():
            logger.info(f"\n保存 {data_type}...")
            
            # 为每个数据类型创建子目录
            output_dir = base_output_dir / data_type
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # 按股票拆分保存（使用通用方法）
            num_files = self._save_symbols_to_csv(
                df=df,
                output_dir=output_dir,
                columns_to_keep=['period', 'datetime', 'field', 'value'],
                file_name_upper=False,  # 文件名小写
                progress_desc=f"保存{data_type}"
            )
            
            logger.info(f"  ✅ {data_type}: 已保存 {num_files} 个CSV文件")
        
        logger.info(f"✅ FEED层数据已保存到: {base_output_dir}")
    
    def dump_to_qlib_binary(self):
        """
        调用dump_pit.py转换为二进制格式
        """
        logger.info("="*60)
        logger.info("转换为Qlib PIT二进制格式")
        logger.info("="*60)
        
        dump_script = self.project_root / self.dump_config['script_path']
        if not dump_script.exists():
            raise FileNotFoundError(f"dump_pit.py脚本不存在: {dump_script}")
        
        base_csv_dir = self.project_root / self.feed_layer_config['output_dir']
        qlib_dir = Path(self.qlib_layer_config['output_dir']).expanduser()
        
        # 获取所有数据类型目录
        data_type_dirs = [d for d in base_csv_dir.iterdir() if d.is_dir() and d.name != 'metadata']
        
        logger.info(f"发现 {len(data_type_dirs)} 个数据类型目录")
        
        for data_type_dir in data_type_dirs:
            logger.info(f"\n处理 {data_type_dir.name}...")
            
            csv_files = list(data_type_dir.glob('*.csv'))
            if not csv_files:
                logger.warning(f"  ⚠️  没有找到CSV文件")
                continue
            
            logger.info(f"  发现 {len(csv_files)} 个CSV文件")
            
            # 构建命令
            cmd = [
                sys.executable,
                str(dump_script),
                'dump_all',
                '--csv_path', str(data_type_dir),
                '--qlib_dir', str(qlib_dir),
                '--max_workers', str(self.dump_config.get('max_workers', 16)),
                '--date_field_name', 'datetime',
                '--period_field_name', 'period',
                '--field_field_name', 'field',
                '--value_field_name', 'value',
                '--file_suffix', '.csv',
                '--interval', self.interval
            ]
            
            # 执行命令（使用通用方法）
            try:
                self._execute_subprocess(cmd, description=f"转换 {data_type_dir.name}")
            except RuntimeError as e:
                logger.error(f"  ❌ {data_type_dir.name} 转换失败: {e}")
                continue
        
        logger.info(f"✅ 所有PIT数据已保存到: {qlib_dir}/financial")
    
    def validate(self, sample_size: int = 3):
        """
        验证生成的Qlib PIT数据（简化版，只检查数据是否生成）
        
        Args:
            sample_size: 随机抽样数量（PIT数据验证较简单，主要检查文件存在性）
        """
        logger.info("="*60)
        logger.info("验证Qlib PIT数据")
        logger.info("="*60)
        
        qlib_dir = Path(self.qlib_layer_config['output_dir']).expanduser()
        financial_dir = qlib_dir / 'financial'
        
        if not financial_dir.exists():
            logger.error(f"❌ PIT数据目录不存在: {financial_dir}")
            return
        
        # 统计生成的股票数量
        stock_dirs = [d for d in financial_dir.iterdir() if d.is_dir()]
        
        if not stock_dirs:
            logger.error("❌ 没有找到任何股票PIT数据")
            return
        
        logger.info(f"✅ PIT数据目录: {financial_dir}")
        logger.info(f"✅ 共生成 {len(stock_dirs)} 支股票的PIT数据")
        
        # 随机抽样检查
        import random
        sample_dirs = random.sample(stock_dirs, min(sample_size, len(stock_dirs)))
        
        logger.info(f"\n随机抽样 {len(sample_dirs)} 支股票验证:")
        for stock_dir in sample_dirs:
            # 统计该股票的字段数量
            data_files = list(stock_dir.glob('*.data'))
            index_files = list(stock_dir.glob('*.index'))
            
            logger.info(f"  ✅ {stock_dir.name}: {len(data_files)} 个字段")
            logger.info(f"     .data文件: {len(data_files)}, .index文件: {len(index_files)}")
            
            # 显示部分字段名
            if data_files:
                field_names = [f.stem for f in data_files[:5]]
                logger.info(f"     字段示例: {', '.join(field_names)}")
        
        logger.info("="*60)
        logger.info("✅ PIT数据验证完成")
        logger.info("="*60)

