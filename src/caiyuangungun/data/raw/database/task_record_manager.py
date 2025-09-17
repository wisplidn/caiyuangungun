#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务执行记录数据库管理器
"""

import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional

import pymysql
from pymysql.cursors import DictCursor

try:
    from core.config_manager import ConfigManager
except ImportError:
    # 备用导入方式
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent.parent))
    from core.config_manager import ConfigManager


class TaskRecordManager:
    """任务执行记录数据库管理器"""
    
    def __init__(self, config_manager: ConfigManager = None):
        self.config_manager = config_manager or ConfigManager()
        self.logger = logging.getLogger(__name__)
        self._connection = None
        
    def _get_db_config(self) -> Dict[str, Any]:
        """获取数据库配置"""
        db_config = self.config_manager.get('database', {})
        
        if not db_config.get('enabled', False):
            raise ValueError("数据库未启用")
            
        return {
            'host': db_config.get('host', 'localhost'),
            'port': db_config.get('port', 3306),
            'user': db_config.get('username', 'root'),
            'password': db_config.get('password', ''),
            'database': db_config.get('database', 'caiyuangungun'),
            'charset': db_config.get('charset', 'utf8mb4'),
            'cursorclass': DictCursor,
            'autocommit': True
        }
    
    def _get_connection(self):
        """获取数据库连接"""
        if self._connection is None or not self._connection.open:
            try:
                db_config = self._get_db_config()
                self._connection = pymysql.connect(**db_config)
                self.logger.info("数据库连接建立成功")
            except Exception as e:
                self.logger.error(f"数据库连接失败: {e}")
                raise
        return self._connection
    
    def close_connection(self):
        """关闭数据库连接"""
        if self._connection and self._connection.open:
            self._connection.close()
            self.logger.info("数据库连接已关闭")
    
    def create_table_if_not_exists(self):
        """创建表（如果不存在）"""
        create_sql = """
        CREATE TABLE IF NOT EXISTS task_execution_records (
            id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '记录ID',
            task_id VARCHAR(255) NOT NULL COMMENT '任务ID',
            source_name VARCHAR(100) NOT NULL COMMENT '数据源名称',
            data_type VARCHAR(100) NOT NULL COMMENT '数据类型',
            data_md5 VARCHAR(32) COMMENT '数据MD5值',
            previous_md5 VARCHAR(32) COMMENT '原数据MD5值',
            duration_ms BIGINT COMMENT '执行时长(毫秒)',
            data_rows INT COMMENT '数据行数',
            original_rows INT COMMENT '原行数',
            row_difference INT COMMENT '行数差额',
            error_message TEXT COMMENT '错误信息',
            api_params JSON COMMENT 'API参数',
            original_filename VARCHAR(500) COMMENT '原数据文件名',
            file_path VARCHAR(1000) COMMENT '数据文件路径',
            archive_path VARCHAR(1000) COMMENT '归档文件路径',
            execution_status ENUM('SUCCESS', 'FAILED', 'PARTIAL', 'SKIPPED') NOT NULL DEFAULT 'SUCCESS' COMMENT '执行状态',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
            
            INDEX idx_task_id (task_id),
            INDEX idx_source_name (source_name),
            INDEX idx_data_type (data_type),
            INDEX idx_created_at (created_at),
            INDEX idx_execution_status (execution_status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='任务执行记录表'
        """
        
        try:
            connection = self._get_connection()
            with connection.cursor() as cursor:
                cursor.execute(create_sql)
            self.logger.info("任务执行记录表创建/检查完成")
        except Exception as e:
            self.logger.error(f"创建表失败: {e}")
            raise
    
    def insert_task_record(self, 
                          task_id: str,
                          source_name: str,
                          data_type: str,
                          data_md5: Optional[str] = None,
                          previous_md5: Optional[str] = None,
                          duration_ms: Optional[int] = None,
                          data_rows: Optional[int] = None,
                          original_rows: Optional[int] = None,
                          row_difference: Optional[int] = None,
                          error_message: Optional[str] = None,
                          api_params: Optional[Dict[str, Any]] = None,
                          original_filename: Optional[str] = None,
                          file_path: Optional[str] = None,
                          archive_path: Optional[str] = None,
                          execution_status: str = 'SUCCESS') -> int:
        """插入任务执行记录
        
        Args:
            task_id: 任务ID
            source_name: 数据源名称
            data_type: 数据类型
            data_md5: 数据MD5值
            previous_md5: 原数据MD5值
            duration_ms: 执行时长(毫秒)
            data_rows: 数据行数
            original_rows: 原行数
            row_difference: 行数差额
            error_message: 错误信息
            api_params: API参数
            original_filename: 原数据文件名
            file_path: 数据文件路径
            archive_path: 归档文件路径
            execution_status: 执行状态
            
        Returns:
            插入记录的ID
        """
        insert_sql = """
        INSERT INTO task_execution_records (
            task_id, source_name, data_type, data_md5, previous_md5,
            duration_ms, data_rows, original_rows, row_difference,
            error_message, api_params, original_filename, file_path, archive_path, execution_status
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        try:
            connection = self._get_connection()
            with connection.cursor() as cursor:
                cursor.execute(insert_sql, (
                    task_id, source_name, data_type, data_md5, previous_md5,
                    duration_ms, data_rows, original_rows, row_difference,
                    error_message, json.dumps(api_params) if api_params else None,
                    original_filename, file_path, archive_path, execution_status
                ))
                record_id = cursor.lastrowid
                
            self.logger.info(f"任务执行记录插入成功，记录ID: {record_id}, 任务ID: {task_id}")
            return record_id
            
        except Exception as e:
            self.logger.error(f"插入任务执行记录失败: {e}")
            raise
    
    def update_task_record(self, record_id: int, **kwargs) -> bool:
        """更新任务执行记录
        
        Args:
            record_id: 记录ID
            **kwargs: 要更新的字段
            
        Returns:
            是否更新成功
        """
        if not kwargs:
            return True
            
        # 构建更新SQL
        set_clauses = []
        values = []
        
        for key, value in kwargs.items():
            if key == 'api_params' and isinstance(value, dict):
                set_clauses.append(f"{key} = %s")
                values.append(json.dumps(value))
            else:
                set_clauses.append(f"{key} = %s")
                values.append(value)
        
        values.append(record_id)
        update_sql = f"UPDATE task_execution_records SET {', '.join(set_clauses)} WHERE id = %s"
        
        try:
            connection = self._get_connection()
            with connection.cursor() as cursor:
                cursor.execute(update_sql, values)
                affected_rows = cursor.rowcount
                
            self.logger.info(f"任务执行记录更新成功，记录ID: {record_id}, 影响行数: {affected_rows}")
            return affected_rows > 0
            
        except Exception as e:
            self.logger.error(f"更新任务执行记录失败: {e}")
            raise
    
    def get_task_record(self, record_id: int) -> Optional[Dict[str, Any]]:
        """获取任务执行记录
        
        Args:
            record_id: 记录ID
            
        Returns:
            任务执行记录
        """
        select_sql = "SELECT * FROM task_execution_records WHERE id = %s"
        
        try:
            connection = self._get_connection()
            with connection.cursor() as cursor:
                cursor.execute(select_sql, (record_id,))
                record = cursor.fetchone()
                
            if record and record.get('api_params'):
                try:
                    record['api_params'] = json.loads(record['api_params'])
                except (json.JSONDecodeError, TypeError):
                    pass
                    
            return record
            
        except Exception as e:
            self.logger.error(f"获取任务执行记录失败: {e}")
            raise
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close_connection()