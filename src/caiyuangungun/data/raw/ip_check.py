#!/usr/bin/env python3
"""
IP地区检测模块

提供上海地区IP检测功能，确保只有上海地区的用户可以访问数据接口
"""

import requests
import sys
import os

# 全局IP检测标志
_ip_checked = False
_ip_valid = False

# 调试模式：设置环境变量 SKIP_IP_CHECK=1 可以跳过IP检测
_SKIP_IP_CHECK = os.environ.get('SKIP_IP_CHECK', '0') == '1'

def check_shanghai_ip():
    """
    检查当前IP是否为上海地区
    
    Returns:
        bool: True if IP is from Shanghai, False otherwise
    """
    try:
        print("🌐 正在检测IP地理位置...")
        
        # 使用多个IP检测服务确保准确性
        ip_services = [
            "http://ip-api.com/json/?lang=zh-CN",
            "https://ipapi.co/json/",
            "http://ipinfo.io/json"
        ]
        
        for service_url in ip_services:
            try:
                response = requests.get(service_url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    
                    # 不同服务的字段名可能不同
                    location_fields = [
                        data.get('regionName', ''),  # ip-api.com
                        data.get('region', ''),      # ipapi.co
                        data.get('region', ''),      # ipinfo.io
                        data.get('city', ''),        # 城市字段
                        data.get('province', ''),    # 省份字段
                    ]
                    
                    location_text = ' '.join(str(field) for field in location_fields).lower()
                    
                    print(f"🔍 检测到位置信息: {location_text}")
                    
                    # 检查是否包含上海相关关键词
                    shanghai_keywords = ['shanghai', '上海', 'sh']
                    is_shanghai = any(keyword in location_text for keyword in shanghai_keywords)
                    
                    if is_shanghai:
                        print("✅ IP检测通过：位于上海地区")
                        return True
                    else:
                        print(f"❌ IP检测失败：不在上海地区 (检测位置: {location_text})")
                        return False
                        
            except Exception as e:
                print(f"⚠️  IP服务 {service_url} 检测失败: {e}")
                continue
        
        print("❌ 所有IP检测服务均失败，拒绝访问")
        return False
        
    except Exception as e:
        print(f"❌ IP检测过程出现错误: {e}")
        return False

def ensure_shanghai_ip():
    """
    确保当前IP为上海地区，如果不是则终止程序
    使用全局变量确保只检测一次
    """
    global _ip_checked, _ip_valid
    
    # 调试模式跳过检测
    if _SKIP_IP_CHECK:
        print("🔧 调试模式：跳过IP检测")
        return
    
    if _ip_checked:
        if not _ip_valid:
            print("❌ IP检测已失败，程序终止")
            sys.exit(1)
        return
    
    print("🚨 首次访问，进行IP地区验证...")
    _ip_checked = True
    _ip_valid = check_shanghai_ip()
    
    if not _ip_valid:
        print("🚫 非上海地区IP，程序终止")
        print("💡 提示：此程序仅允许在上海地区使用")
        print("🔧 调试提示：设置环境变量 SKIP_IP_CHECK=1 可以跳过检测")
        sys.exit(1)
    
    print("🎉 IP验证通过，允许继续访问")

def get_ip_status():
    """
    获取当前IP检测状态
    
    Returns:
        dict: IP检测状态信息
    """
    return {
        'checked': _ip_checked,
        'valid': _ip_valid,
        'skip_enabled': _SKIP_IP_CHECK
    }
