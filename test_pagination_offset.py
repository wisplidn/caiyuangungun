#!/usr/bin/env python3
"""
测试利润表VIP接口的分页逻辑和offset设置

验证offset=9000时是否还有重复行，以及完整的分页获取流程
"""

import sys
import time
import pandas as pd
from pathlib import Path

# 添加项目路径
current_dir = Path(__file__).parent
project_root = current_dir
sys.path.insert(0, str(project_root / "src"))

from caiyuangungun.data.raw.client import get_tushare_data

def test_single_offset(offset, start_date, end_date, max_retries=3):
    """
    测试单个offset值的API调用
    
    Args:
        offset: 偏移量
        start_date: 开始日期
        end_date: 结束日期  
        max_retries: 最大重试次数
    
    Returns:
        tuple: (DataFrame, success_flag, error_message)
    """
    for attempt in range(max_retries):
        try:
            print(f"    📡 尝试 {attempt + 1}/{max_retries}: offset={offset}")
            
            df = get_tushare_data(
                'income_vip', 
                start_date=start_date, 
                end_date=end_date,
                offset=str(offset)
            )
            
            print(f"    ✅ 成功获取 {len(df)} 条记录")
            return df, True, None
            
        except Exception as e:
            error_msg = str(e)
            print(f"    ❌ 第 {attempt + 1} 次尝试失败:")
            print(f"       错误类型: {type(e).__name__}")
            print(f"       错误信息: {error_msg}")
            
            if attempt < max_retries - 1:
                # 如果是429错误，等待更长时间
                if "429" in error_msg or "多台设备" in error_msg:
                    wait_time = 30  # 等待30秒
                    print(f"    ⏳ 检测到限流错误，等待 {wait_time} 秒后重试...")
                else:
                    wait_time = 3
                    print(f"    ⏳ 等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                print(f"    🚫 达到最大重试次数，放弃")
                return pd.DataFrame(), False, error_msg
    
    return pd.DataFrame(), False, "未知错误"

def test_pagination_logic():
    """测试分页逻辑 - 动态计算offset"""
    print("🧪 测试利润表VIP接口分页逻辑（动态offset）")
    print("=" * 60)
    
    # 测试参数
    start_date = '20240401'
    end_date = '20240430'
    max_pages = 5
    overlap_size = 100  # 重叠行数
    
    all_data = []
    success_count = 0
    current_offset = 0
    
    print(f"📅 测试日期范围: {start_date} - {end_date}")
    print(f"🔢 最大页数: {max_pages}")
    print(f"🔄 重叠行数: {overlap_size}")
    print()
    
    for page_num in range(1, max_pages + 1):
        print(f"--- 测试第 {page_num} 页 (offset={current_offset}) ---")
        
        df, success, error = test_single_offset(current_offset, start_date, end_date)
        
        if success and len(df) > 0:
            # 添加分页元数据
            df['_offset'] = current_offset
            df['_page'] = page_num
            df['_record_count'] = len(df)
            all_data.append(df)
            success_count += 1
            
            print(f"    📊 数据详情:")
            print(f"       记录数: {len(df)}")
            print(f"       股票数: {df['ts_code'].nunique()}")
            print(f"       公告日期范围: {df['ann_date'].min()} - {df['ann_date'].max()}")
            
            # 计算下一页的offset
            next_offset = current_offset + len(df) - overlap_size
            print(f"    📝 下一页offset计算: {current_offset} + {len(df)} - {overlap_size} = {next_offset}")
            
            # 如果这页数据少于4000，说明可能是最后一页
            if len(df) < 4000:
                print(f"    🏁 本页数据量 < 4000，可能是最后一页")
                break
            
            current_offset = next_offset
            
        else:
            print(f"    ⚠️  无数据或获取失败")
            if error:
                print(f"       最终错误: {error}")
            break
        
        print()
    
    # 合并所有数据
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        
        print("📊 合并数据分析:")
        print(f"   总记录数: {len(combined_df)}")
        print(f"   成功页数: {success_count}/{len(test_offsets)}")
        print(f"   股票代码数: {combined_df['ts_code'].nunique()}")
        
        # 重复性检查
        print(f"\n🔍 重复性分析:")
        total_records = len(combined_df)
        unique_records = len(combined_df.drop_duplicates(subset=['ts_code', 'ann_date', 'end_date']))
        duplicate_records = total_records - unique_records
        
        print(f"   总记录数: {total_records}")
        print(f"   唯一记录数: {unique_records}")
        print(f"   重复记录数: {duplicate_records}")
        print(f"   重复率: {duplicate_records/total_records*100:.1f}%")
        
        # 按页分析重复情况
        print(f"\n📄 按页重复分析:")
        for page in sorted(combined_df['_page'].unique()):
            page_data = combined_df[combined_df['_page'] == page]
            page_offset = page_data['_offset'].iloc[0]
            
            # 检查与前面所有页的重复
            previous_data = combined_df[combined_df['_page'] < page]
            if len(previous_data) > 0:
                page_keys = set(page_data['ts_code'] + '_' + page_data['ann_date'].astype(str) + '_' + page_data['end_date'].astype(str))
                prev_keys = set(previous_data['ts_code'] + '_' + previous_data['ann_date'].astype(str) + '_' + previous_data['end_date'].astype(str))
                overlap = len(page_keys.intersection(prev_keys))
                print(f"   第{page}页 (offset={page_offset}): {len(page_data)}条记录, 与前面页重复{overlap}条")
            else:
                print(f"   第{page}页 (offset={page_offset}): {len(page_data)}条记录, 首页无重复")
        
        # 保存结果
        output_file = project_root / "pagination_test_result.parquet"
        combined_df.to_parquet(output_file, index=False)
        print(f"\n💾 数据已保存到: {output_file}")
        
        # 保存去重后的数据
        unique_df = combined_df.drop_duplicates(subset=['ts_code', 'ann_date', 'end_date'])
        unique_output_file = project_root / "pagination_test_result_unique.parquet"
        unique_df.to_parquet(unique_output_file, index=False)
        print(f"💾 去重数据已保存到: {unique_output_file}")
        
        return combined_df
    else:
        print("❌ 未获取到任何数据")
        return pd.DataFrame()

if __name__ == "__main__":
    try:
        result_df = test_pagination_logic()
        print("\n🎉 测试完成！")
    except KeyboardInterrupt:
        print("\n⚠️  用户中断测试")
    except Exception as e:
        print(f"\n❌ 测试过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
