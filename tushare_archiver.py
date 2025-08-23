"""
Tushare数据归档器 (Tushare Archiver)
基于README规划，实现Tushare数据的历史回填和增量更新机制。
支持多种数据类型（如income, balancesheet等），使用Parquet格式存储，
并提供版本管理和Point-in-Time查询支持。
"""


import pandas as pd
import hashlib
import json
from datetime import datetime
from pathlib import Path
import sqlite3
import tushare_client

class TushareArchiver:
    def __init__(self, data_type, base_path="./data"):
        vip_func_name = f"get_{data_type}_vip"
        std_func_name = f"get_{data_type}"

        if hasattr(tushare_client, vip_func_name):
            self.fetch_function_name = vip_func_name
        elif hasattr(tushare_client, std_func_name):
            self.fetch_function_name = std_func_name
        else:
            raise ValueError(f"Unsupported data_type: '{data_type}'. No corresponding function found in tushare_client.")
        self.fetch_function = getattr(tushare_client, self.fetch_function_name)

        self.data_type = data_type
        self.base_path = Path(base_path)
        self.raw_landing_path = self.base_path / "raw" / "landing" / "tushare" / self.data_type
        self.raw_norm_path = self.base_path / "raw" / "norm" / "tushare" / self.data_type
        self.log_db_path = self.base_path / "logs" / "request_log.db"
        

        # 创建目录结构和数据库
        self._create_directories()
        self._init_log_db()



    def _create_directories(self):
        """创建目录结构"""
        self.raw_landing_path.mkdir(parents=True, exist_ok=True)
        self.raw_norm_path.mkdir(parents=True, exist_ok=True)
        self.log_db_path.parent.mkdir(parents=True, exist_ok=True)

    def _init_log_db(self):
        """初始化请求日志数据库"""
        conn = sqlite3.connect(self.log_db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS request_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_type TEXT NOT NULL,
                period TEXT NOT NULL,
                ingest_date TEXT NOT NULL,
                params TEXT,
                row_count INTEGER,
                checksum TEXT,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(data_type, period, ingest_date)
            )
        """)
        conn.commit()
        conn.close()

    def _calculate_checksum(self, df):
        """计算DataFrame的稳定checksum"""
        if df.empty:
            return "empty"

        # 排序确保稳定性
        # 动态选择存在的列进行排序，以适应不同接口返回的字段
        potential_keys = ['ts_code', 'end_date', 'ann_date', 'report_type', 'comp_type', 'f_ann_date']
        sort_keys = [key for key in potential_keys if key in df.columns]
        if not sort_keys:
            # 如果没有任何预期的排序列，则按所有列排序以确保一致性
            sort_keys = sorted(df.columns.tolist())

        df_sorted = df.sort_values(by=sort_keys).reset_index(drop=True)

        # 转换为字符串并计算hash
        content = df_sorted.to_csv(index=False, float_format='%.6f')
        return hashlib.md5(content.encode()).hexdigest()

    def _log_request(self, period, ingest_date, params, row_count, checksum, status):
        """记录请求日志"""
        conn = sqlite3.connect(self.log_db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO request_log
            (data_type, period, ingest_date, params, row_count, checksum, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (self.data_type, period, ingest_date, json.dumps(params), row_count, checksum, status))
        conn.commit()
        conn.close()

    def _get_last_checksum(self, period):
        """获取指定period的最新checksum"""
        conn = sqlite3.connect(self.log_db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT checksum FROM request_log
            WHERE data_type = ? AND period = ? AND status IN ('success', 'updated', 'no_change')
            ORDER BY ingest_date DESC LIMIT 1
        """, (self.data_type, period))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None

    def _save_to_landing(self, df, period, ingest_date):
        """将原始数据保存到Landing层"""
        base_path = self.raw_landing_path

        period_path = base_path / f"period={period}"
        ingest_path = period_path / f"ingest_date={ingest_date}"
        ingest_path.mkdir(parents=True, exist_ok=True)

        # 保存数据文件
        data_file = ingest_path / "data.parquet"
        df.to_parquet(data_file, compression='snappy', index=False)

        # 保存元数据
        metadata = {
            "period": period,
            "ingest_date": ingest_date,
            "row_count": len(df),
            "checksum": self._calculate_checksum(df),
            "created_at": datetime.now().isoformat(),
            "fields": list(df.columns)
        }

        metadata_file = ingest_path / "metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)

        return data_file

    def generate_quarters(self, start_year=2006, start_quarter=1):
        """生成季度列表，从2006Q1开始到当前季度"""
        quarters = []
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month
        current_quarter = (current_month - 1) // 3 + 1

        year = start_year
        quarter = start_quarter

        while year < current_year or (year == current_year and quarter <= current_quarter):
            if quarter == 1:
                period = f"{year}0331"
            elif quarter == 2:
                period = f"{year}0630"
            elif quarter == 3:
                period = f"{year}0930"
            else:  # quarter == 4
                period = f"{year}1231"

            quarters.append(period)

            quarter += 1
            if quarter > 4:
                quarter = 1
                year += 1

        return quarters

    def fetch_data(self, period):
        """获取指定期间的数据，并返回数据、参数和获取状态"""
        print(f"Fetching {self.data_type} data for period: {period}")
        params = {'period': period}

        # 调用客户端函数，它现在总是返回 (df, status)
        df, status = self.fetch_function(period=period)

        if status == 'error':
            # 错误已在客户端打印，这里只传递状态
            return pd.DataFrame(), params, 'error'

        # 数据清洗和标准化 (仅在成功时执行)
        if not df.empty:
            df = df.replace({pd.NA: None, pd.NaT: None}).where(pd.notnull(df), None)
            print(f"Successfully fetched {len(df)} records for period {period}")
        else:
            print(f"No data returned for period {period}")

        return df, params, 'success'

    def historical_backfill(self):
        """历史数据回填 - 从2006Q1开始逐季度获取"""
        print("Starting historical backfill...")

        quarters = self.generate_quarters()
        ingest_date = datetime.now().strftime('%Y-%m-%d')

        for period in quarters:
            # 检查是否已存在数据
            period_path = self.raw_landing_path / f"period={period}"
            if period_path.exists():
                print(f"Period {period} already exists, skipping...")
                continue

            # 获取数据
            df, params, fetch_status = self.fetch_data(period)

            if fetch_status == 'error':
                self._log_request(period, ingest_date, params, 0, "error", fetch_status)
                continue

            if df.empty:
                # 记录空结果
                self._log_request(period, ingest_date, params, 0, "empty", "no_data")
                continue

            # 计算checksum
            checksum = self._calculate_checksum(df)

            # 保存数据
            try:
                self._save_to_landing(df, period, ingest_date)

                # 记录成功日志
                self._log_request(period, ingest_date, params, len(df), checksum, "success")
                print(f"Successfully saved {len(df)} records for period {period}")

            except Exception as e:
                print(f"Error saving data for period {period}: {e}")
                self._log_request(period, ingest_date, params, len(df), checksum, "error")

    def incremental_update(self, lookback_quarters=12):
        """增量更新 - 滑动窗口检查"""
        print(f"Starting incremental update with lookback of {lookback_quarters} quarters...")

        # 获取最近N个季度
        all_quarters = self.generate_quarters()
        recent_quarters = all_quarters[-lookback_quarters:] if len(all_quarters) > lookback_quarters else all_quarters

        ingest_date = datetime.now().strftime('%Y-%m-%d')

        for period in recent_quarters:
            print(f"Checking period {period}...")

            # 获取数据
            df, params, fetch_status = self.fetch_data(period)

            if fetch_status == 'error':
                self._log_request(period, ingest_date, params, 0, "error", fetch_status)
                continue

            if df.empty:
                self._log_request(period, ingest_date, params, 0, "empty", "no_data")
                continue

            # 计算新的checksum
            new_checksum = self._calculate_checksum(df)

            # 获取上一次的checksum
            last_checksum = self._get_last_checksum(period)

            if new_checksum == last_checksum:
                # 无变更，只记录日志
                self._log_request(period, ingest_date, params, len(df), new_checksum, "no_change")
                print(f"No changes for period {period}")
            else:
                # 有变更，保存新版本
                try:
                    self._save_to_landing(df, period, ingest_date)

                    self._log_request(period, ingest_date, params, len(df), new_checksum, "updated")
                    print(f"Updated period {period} with {len(df)} records")

                except Exception as e:
                    print(f"Error updating period {period}: {e}")
                    self._log_request(period, ingest_date, params, len(df), new_checksum, "error")

if __name__ == '__main__':
    # 此主程序块仅用于测试和演示
    # 实际调度应通过 main.py 或外部任务管理器进行
    print("Testing TushareArchiver for 'income' data_type...")
    try:
        income_archiver = TushareArchiver(data_type='income')

        # 检查是否需要历史回填
        if not any(income_archiver.raw_landing_path.iterdir()):
            print("No income data found. Starting historical backfill...")
            income_archiver.historical_backfill()
        else:
            print("Income data detected. Running incremental update...")
            income_archiver.incremental_update()
        print("\nTest complete.")
    except ValueError as e:
        print(f"Error: {e}")
