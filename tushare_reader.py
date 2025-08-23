"""
Tushare数据读取器 (Tushare Reader)
支持Point-in-Time查询和最新数据读取，可处理多种数据类型。
"""

import pandas as pd
import sqlite3
from pathlib import Path

import json

class TushareReader:
    def __init__(self, data_type, base_path="./data"):
        self.data_type = data_type
        self.base_path = Path(base_path)
        self.raw_landing_path = self.base_path / "raw" / "landing" / "tushare" / self.data_type
        self.raw_norm_path = self.base_path / "raw" / "norm" / "tushare" / self.data_type
        self.log_db_path = self.base_path / "logs" / "request_log.db"

    def get_available_periods(self):
        """获取所有可用的期间"""
        periods = []
        if self.raw_landing_path.exists():
            for period_dir in self.raw_landing_path.iterdir():
                if period_dir.is_dir() and period_dir.name.startswith("period="):
                    period = period_dir.name.replace("period=", "")
                    periods.append(period)
        return sorted(periods)

    def get_available_versions(self, period):
        """获取指定期间的所有版本（ingest_date）"""
        period_path = self.raw_landing_path / f"period={period}"
        versions = []
        if period_path.exists():
            for version_dir in period_path.iterdir():
                if version_dir.is_dir() and version_dir.name.startswith("ingest_date="):
                    ingest_date = version_dir.name.replace("ingest_date=", "")
                    versions.append(ingest_date)
        return sorted(versions)

    def read_latest_data(self, period):
        """读取指定期间的最新数据"""
        versions = self.get_available_versions(period)
        if not versions:
            return pd.DataFrame()
        
        latest_version = versions[-1]
        return self.read_data(period, latest_version)

    def read_data(self, period, ingest_date):
        """读取指定期间和版本的数据"""
        data_path = self.raw_norm_path / f"period={period}" / f"ingest_date={ingest_date}" / "data.parquet"
        
        if not data_path.exists():
            # 如果norm路径不存在，尝试landing路径
            data_path = self.raw_landing_path / f"period={period}" / f"ingest_date={ingest_date}" / "data.parquet"
        
        if data_path.exists():
            return pd.read_parquet(data_path)
        else:
            return pd.DataFrame()

    def read_pit_data(self, period, as_of_date):
        """Point-in-Time读取：读取截至指定日期的最新版本数据"""
        versions = self.get_available_versions(period)
        if not versions:
            return pd.DataFrame()
        
        # 找到小于等于as_of_date的最新版本
        valid_versions = [v for v in versions if v <= as_of_date]
        if not valid_versions:
            return pd.DataFrame()
        
        pit_version = valid_versions[-1]
        return self.read_data(period, pit_version)

    def read_all_latest(self):
        """读取所有期间的最新数据"""
        all_data = []
        periods = self.get_available_periods()
        
        for period in periods:
            df = self.read_latest_data(period)
            if not df.empty:
                all_data.append(df)
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()

    def read_multiple_periods(self, periods, as_of_date=None):
        """读取多个期间的数据"""
        all_data = []
        
        for period in periods:
            if as_of_date:
                df = self.read_pit_data(period, as_of_date)
            else:
                df = self.read_latest_data(period)
            
            if not df.empty:
                all_data.append(df)
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()

    def get_metadata(self, period, ingest_date):
        """获取指定版本的元数据"""
        metadata_path = self.raw_norm_path / f"period={period}" / f"ingest_date={ingest_date}" / "metadata.json"
        
        if not metadata_path.exists():
            metadata_path = self.raw_landing_path / f"period={period}" / f"ingest_date={ingest_date}" / "metadata.json"
        
        if metadata_path.exists():
            with open(metadata_path, 'r') as f:
                return json.load(f)
        else:
            return {}

    def get_request_log(self, period=None, limit=100):
        """获取请求日志. 设置 limit=None 可获取所有日志."""
        if not self.log_db_path.exists():
            return pd.DataFrame()

        conn = sqlite3.connect(self.log_db_path)
        params = [self.data_type]

        if period:
            query = "SELECT * FROM request_log WHERE data_type = ? AND period = ? ORDER BY created_at DESC"
            params.append(period)
        else:
            query = "SELECT * FROM request_log WHERE data_type = ? ORDER BY created_at DESC"

        if limit and limit > 0:
            query += " LIMIT ?"
            params.append(limit)

        df = pd.read_sql_query(query, conn, params=tuple(params))
        conn.close()
        return df

    def get_data_summary(self):
        """获取数据概要统计"""
        periods = self.get_available_periods()
        summary = []
        
        for period in periods:
            versions = self.get_available_versions(period)
            if versions:
                latest_version = versions[-1]
                metadata = self.get_metadata(period, latest_version)
                summary.append({
                    'period': period,
                    'latest_ingest_date': latest_version,
                    'version_count': len(versions),
                    'row_count': metadata.get('row_count', 0),
                    'checksum': metadata.get('checksum', ''),
                    'created_at': metadata.get('created_at', '')
                })
        
        return pd.DataFrame(summary)

    def search_stocks(self, ts_codes, periods=None, as_of_date=None):
        """搜索特定股票的数据"""
        if periods is None:
            periods = self.get_available_periods()
        
        df = self.read_multiple_periods(periods, as_of_date)
        
        if not df.empty and 'ts_code' in df.columns:
            return df[df['ts_code'].isin(ts_codes)]
        else:
            return pd.DataFrame()

    def get_latest_quarter_data(self, quarters_back=1):
        """获取最近N个季度的数据"""
        periods = self.get_available_periods()
        if not periods:
            return pd.DataFrame()
        
        recent_periods = periods[-quarters_back:] if len(periods) >= quarters_back else periods
        return self.read_multiple_periods(recent_periods)

# 使用示例
if __name__ == '__main__':
    # 此主程序块仅用于测试和演示
    print("Testing TushareReader for 'income' data_type...")
    try:
        income_reader = TushareReader(data_type='income')

        # 获取数据概要
        print("\n=== Income Data Summary ===")
        summary = income_reader.get_data_summary()
        if not summary.empty:
            print(summary.to_string(index=False))
        else:
            print("No summary available.")

        # 获取最新季度数据
        print("\n=== Latest Quarter Data Sample ===")
        latest_data = income_reader.get_latest_quarter_data(1)
        if not latest_data.empty:
            print(f"Rows in latest quarter: {len(latest_data)}")
            # Display common columns, handle if they don't exist
            display_cols = [col for col in ['ts_code', 'end_date', 'total_revenue', 'n_income'] if col in latest_data.columns]
            print(latest_data[display_cols].head())
        else:
            print("No latest quarter data found.")

        # 获取请求日志
        print("\n=== Recent Request Logs for Income ===")
        logs = income_reader.get_request_log(limit=10)
        if not logs.empty:
            print(logs[['period', 'ingest_date', 'row_count', 'status']].to_string(index=False))
        else:
            print("No logs found for income.")

        print("\nTest complete.")
    except Exception as e:
        print(f"An error occurred during testing: {e}")
