#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CLI配置管理模块
支持从配置文件读取默认参数
"""

import json
import yaml
from pathlib import Path
from typing import Dict, Any, Optional


class CLIConfig:
    """CLI配置管理器"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config_file = config_file
        self.config_data = {}
        
        if config_file:
            self.load_config(config_file)
    
    def load_config(self, config_file: str) -> None:
        """加载配置文件"""
        config_path = Path(config_file)
        
        if not config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_file}")
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                if config_path.suffix.lower() in ['.yml', '.yaml']:
                    self.config_data = yaml.safe_load(f) or {}
                elif config_path.suffix.lower() == '.json':
                    self.config_data = json.load(f)
                else:
                    raise ValueError(f"不支持的配置文件格式: {config_path.suffix}")
        except Exception as e:
            raise ValueError(f"加载配置文件失败: {e}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值"""
        keys = key.split('.')
        value = self.config_data
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_default_sources(self) -> list:
        """获取默认数据源列表"""
        return self.get('defaults.sources', [])
    
    def get_default_methods(self) -> list:
        """获取默认方法列表"""
        return self.get('defaults.methods', [])
    
    def get_default_storage_types(self) -> list:
        """获取默认存储类型列表"""
        return self.get('defaults.storage_types', [])
    
    def get_default_date_range(self) -> dict:
        """获取默认日期范围"""
        return self.get('defaults.date_range', {})
    
    def get_profiles(self) -> dict:
        """获取预设配置文件"""
        return self.get('profiles', {})
    
    def get_profile(self, profile_name: str) -> dict:
        """获取指定的预设配置"""
        profiles = self.get_profiles()
        return profiles.get(profile_name, {})


def create_sample_config() -> str:
    """创建示例配置文件内容"""
    sample_config = {
        "defaults": {
            "sources": ["tushare"],
            "methods": ["stock_basic"],
            "storage_types": ["SNAPSHOT"],
            "date_range": {
                "days_back": 7
            },
            "force_update": False,
            "lookback_multiplier": 0
        },
        "profiles": {
            "stock_basic": {
                "description": "股票基础信息采集",
                "sources": ["tushare"],
                "methods": ["stock_basic"],
                "storage_types": ["SNAPSHOT"],
                "force_update": True
            },
            "daily_data": {
                "description": "日线数据采集",
                "sources": ["tushare", "akshare"],
                "methods": ["daily"],
                "storage_types": ["PERIOD"],
                "date_range": {
                    "days_back": 30
                }
            },
            "trade_calendar": {
                "description": "交易日历数据",
                "sources": ["tushare"],
                "methods": ["trade_cal"],
                "storage_types": ["SNAPSHOT"],
                "force_update": True
            }
        },
        "output": {
            "default_format": "json",
            "default_directory": "./output",
            "timestamp_format": "%Y%m%d_%H%M%S"
        },
        "logging": {
            "level": "INFO",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    }
    
    return json.dumps(sample_config, ensure_ascii=False, indent=2)


def save_sample_config(file_path: str) -> None:
    """保存示例配置文件"""
    config_content = create_sample_config()
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(config_content)
    
    print(f"示例配置文件已保存到: {file_path}")


if __name__ == '__main__':
    # 创建示例配置文件
    import sys
    
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = 'cli_config.json'
    
    save_sample_config(config_file)