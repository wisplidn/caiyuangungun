"""数据库记录器

用于记录数据获取过程的运行状态、时间和结果信息到MySQL数据库。
支持记录每次数据获取请求的详细信息，用于后续工作流的创建和监控。
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from dataclasses import dataclass

try:
    import pymysql
except ImportError:
    pymysql = None

# 使用动态导入避免相对导入问题
import importlib.util
from pathlib import Path

# 获取config_manager模块
current_dir = Path(__file__).parent
config_manager_path = current_dir.parent / 'core' / 'config_manager.py'

if config_manager_path.exists():
    spec = importlib.util.spec_from_file_location("config_manager", config_manager_path)
    config_manager_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config_manager_module)
    ConfigManager = config_manager_module.ConfigManager
else:
    # 如果找不到文件，定义一个简单的替代类
    class ConfigManager:
        def get_section(self, section):
            return None


@dataclass
class DataFetchRecord:
    """数据获取记录"""
    source_name: str
    data_type: str
    status: str  # 'success', 'failed', 'skipped'
    start_time: datetime
    end_time: Optional[datetime] = None
    data_md5: Optional[str] = None
    previous_md5: Optional[str] = None
    data_shape: Optional[tuple] = None
    previous_data_shape: Optional[tuple] = None
    error_message: Optional[str] = None
    constructed_params: Optional[Dict[str, Any]] = None
    api_params: Optional[Dict[str, Any]] = None
    archive_type: Optional[str] = None
    date_param: Optional[str] = None


class DatabaseLogger:
    """数据库记录器
    
    记录数据获取过程的详细信息到MySQL数据库。
    """
    
    def __init__(self, config_manager: Optional[ConfigManager] = None):
        """初始化数据库记录器
        
        Args:
            config_manager: 配置管理器实例
        """
        self.config_manager = config_manager or ConfigManager()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.connection = None
        
        # 检查pymysql是否可用
        if pymysql is None:
            self.logger.warning("pymysql未安装，数据库记录功能将被禁用")
            self.enabled = False
        else:
            # 获取数据库配置
            db_config = self.config_manager.get_section('database')
            if db_config and db_config.get('enabled', False):
                self.db_config = db_config
                self.enabled = True
                self._init_database()
            else:
                self.logger.info("数据库配置未启用，数据库记录功能将被禁用")
                self.enabled = False
    
    def _init_database(self):
        """初始化数据库连接和表结构"""
        try:
            self._connect()
            self._create_tables()
            self.logger.info("数据库记录器初始化成功")
        except Exception as e:
            self.logger.error(f"数据库记录器初始化失败: {e}")
            self.enabled = False
    
    def _connect(self):
        """建立数据库连接"""
        if not self.enabled:
            return
            
        try:
            self.connection = pymysql.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['username'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset=self.db_config.get('charset', 'utf8mb4'),
                autocommit=True
            )
        except Exception as e:
            self.logger.error(f"数据库连接失败: {e}")
            self.enabled = False
            raise
    
    def _create_tables(self):
        """创建数据获取记录表"""
        if not self.enabled or not self.connection:
            return
            
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS data_fetch_records (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            source_name VARCHAR(50) NOT NULL,
            data_type VARCHAR(50) NOT NULL,
            status ENUM('success', 'failed', 'skipped') NOT NULL,
            start_time DATETIME NOT NULL,
            end_time DATETIME,
            duration_ms INT,
            data_md5 VARCHAR(32),
            previous_md5 VARCHAR(32),
            data_shape JSON,
            previous_data_shape JSON,
            error_message TEXT,
            constructed_params JSON,
            api_params JSON,
            archive_type VARCHAR(20),
            date_param VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_source_type (source_name, data_type),
            INDEX idx_status (status),
            INDEX idx_start_time (start_time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_sql)
            self.logger.info("数据获取记录表创建/检查完成")
        except Exception as e:
            self.logger.error(f"创建数据表失败: {e}")
            raise
    
    def record_fetch_start(self, source_name: str, data_type: str, 
                          constructed_params: Optional[Dict[str, Any]] = None,
                          api_params: Optional[Dict[str, Any]] = None) -> DataFetchRecord:
        """记录数据获取开始
        
        Args:
            source_name: 数据源名称
            data_type: 数据类型
            constructed_params: 程序构造的参数
            api_params: API调用参数
            
        Returns:
            DataFetchRecord: 数据获取记录对象
        """
        record = DataFetchRecord(
            source_name=source_name,
            data_type=data_type,
            status='running',
            start_time=datetime.now(),
            constructed_params=constructed_params,
            api_params=api_params
        )
        
        self.logger.info(f"开始记录数据获取: {source_name}.{data_type}")
        return record
    
    def record_fetch_success(self, record: DataFetchRecord, 
                           data_md5: str, data_shape: tuple,
                           previous_md5: Optional[str] = None,
                           previous_data_shape: Optional[tuple] = None,
                           archive_type: Optional[str] = None,
                           date_param: Optional[str] = None):
        """记录数据获取成功
        
        Args:
            record: 数据获取记录对象
            data_md5: 数据MD5值
            data_shape: 数据形状
            previous_md5: 之前的MD5值
            previous_data_shape: 之前的数据形状
            archive_type: 归档类型
            date_param: 日期参数
        """
        record.status = 'success'
        record.end_time = datetime.now()
        record.data_md5 = data_md5
        record.previous_md5 = previous_md5
        record.data_shape = data_shape
        record.previous_data_shape = previous_data_shape
        record.archive_type = archive_type
        record.date_param = date_param
        
        self._save_record(record)
        self.logger.info(f"记录数据获取成功: {record.source_name}.{record.data_type}")
    
    def record_fetch_failed(self, record: DataFetchRecord, error_message: str):
        """记录数据获取失败
        
        Args:
            record: 数据获取记录对象
            error_message: 错误信息
        """
        record.status = 'failed'
        record.end_time = datetime.now()
        record.error_message = error_message
        
        self._save_record(record)
        self.logger.info(f"记录数据获取失败: {record.source_name}.{record.data_type}")
    
    def record_fetch_skipped(self, record: DataFetchRecord, reason: str = "数据未变更", 
                           data_md5: Optional[str] = None, data_shape: Optional[tuple] = None):
        """记录数据获取跳过
        
        Args:
            record: 数据获取记录对象
            reason: 跳过原因
            data_md5: 数据MD5值
            data_shape: 数据形状
        """
        record.status = 'skipped'
        record.end_time = datetime.now()
        record.error_message = reason
        if data_md5:
            record.data_md5 = data_md5
        if data_shape:
            record.data_shape = data_shape
        
        self._save_record(record)
        self.logger.info(f"记录数据获取跳过: {record.source_name}.{record.data_type}")
    
    def _save_record(self, record: DataFetchRecord):
        """保存记录到数据库
        
        Args:
            record: 数据获取记录对象
        """
        if not self.enabled or not self.connection:
            return
            
        try:
            # 计算执行时间
            duration_ms = None
            if record.end_time and record.start_time:
                duration_ms = int((record.end_time - record.start_time).total_seconds() * 1000)
            
            insert_sql = """
            INSERT INTO data_fetch_records (
                source_name, data_type, status, start_time, end_time, duration_ms,
                data_md5, previous_md5, data_shape, previous_data_shape, error_message,
                constructed_params, api_params, archive_type, date_param
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            with self.connection.cursor() as cursor:
                cursor.execute(insert_sql, (
                    record.source_name,
                    record.data_type,
                    record.status,
                    record.start_time,
                    record.end_time,
                    duration_ms,
                    record.data_md5,
                    record.previous_md5,
                    json.dumps(record.data_shape) if record.data_shape else None,
                    json.dumps(record.previous_data_shape) if record.previous_data_shape else None,
                    record.error_message,
                    json.dumps(record.constructed_params) if record.constructed_params else None,
                    json.dumps(record.api_params) if record.api_params else None,
                    record.archive_type,
                    record.date_param
                ))
                
        except Exception as e:
            self.logger.error(f"保存数据获取记录失败: {e}")
    
    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            self.connection = None