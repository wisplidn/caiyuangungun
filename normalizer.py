"""
Tushare数据规范化器 (Tushare Normalizer)

负责从 landing 层读取原始数据，执行清洗和规范化操作，
然后将结果保存到 norm 层。
"""

import pandas as pd
from pathlib import Path
import json
from datetime import datetime

class TushareNormalizer:
    def __init__(self, data_type, base_path="./data"):
        self.data_type = data_type
        self.base_path = Path(base_path)
        self.landing_path = self.base_path / "raw" / "landing" / "tushare" / self.data_type
        self.norm_path = self.base_path / "raw" / "norm" / "tushare" / self.data_type

        self.norm_path.mkdir(parents=True, exist_ok=True)

    def find_unprocessed_data(self):
        """查找所有在 landing 层但不在 norm 层的 ingest 版本"""
        unprocessed = []
        if not self.landing_path.exists():
            return unprocessed

        for period_dir in self.landing_path.iterdir():
            if not (period_dir.is_dir() and period_dir.name.startswith("period=")):
                continue
            
            for ingest_dir in period_dir.iterdir():
                if not (ingest_dir.is_dir() and ingest_dir.name.startswith("ingest_date=")):
                    continue
                
                norm_version_path = self.norm_path / period_dir.name / ingest_dir.name
                if not norm_version_path.exists():
                    unprocessed.append((period_dir.name, ingest_dir.name))
        return unprocessed

    def _normalize(self, df):
        """执行数据规范化操作（当前为占位符）"""
        print("Normalizing data... (current step is a placeholder)")
        # 1. 统一数据类型 (示例)
        # for col in df.select_dtypes(include=['object']).columns:
        #     df[col] = df[col].astype('string')

        # 2. 统一处理缺失值 (示例)
        # df = df.replace(['None', 'NULL'], pd.NA)

        # 3. 统一列名 (未来)

        # 当前只返回原始副本
        return df.copy()

    def process(self, period, ingest_date):
        """处理单个 ingest 版本"""
        landing_file = self.landing_path / period / ingest_date / "data.parquet"
        if not landing_file.exists():
            print(f"Source file not found: {landing_file}")
            return

        print(f"Processing {self.data_type}/{period}/{ingest_date}...")
        df = pd.read_parquet(landing_file)

        # 执行规范化
        df_normalized = self._normalize(df)

        # 保存到 norm 层
        norm_dir = self.norm_path / period / ingest_date
        norm_dir.mkdir(parents=True, exist_ok=True)
        df_normalized.to_parquet(norm_dir / "data.parquet", compression='snappy', index=False)

        print(f"Successfully saved normalized data to {norm_dir}")

    def run(self):
        """运行规范化流程，处理所有未处理的数据"""
        unprocessed_items = self.find_unprocessed_data()
        if not unprocessed_items:
            print(f"All {self.data_type} data is already normalized.")
            return

        print(f"Found {len(unprocessed_items)} unprocessed data versions for '{self.data_type}'.")
        for period, ingest_date in unprocessed_items:
            self.process(period, ingest_date)
        print("Normalization run complete.")

if __name__ == '__main__':
    print("Testing TushareNormalizer for 'income' data_type...")
    normalizer = TushareNormalizer(data_type='income')
    normalizer.run()
