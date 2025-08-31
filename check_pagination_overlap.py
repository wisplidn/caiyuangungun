#!/usr/bin/env python3
"""
检查分页测试结果中的重叠情况

用于验证分页逻辑是否正确，特别是检查相邻页面之间是否有预期的100行重叠
"""

import pandas as pd
from pathlib import Path

def analyze_pagination_overlap(parquet_file):
    """分析分页重叠情况"""
    
    if not Path(parquet_file).exists():
        print(f"❌ 文件不存在: {parquet_file}")
        return
    
    print("🔍 分析分页重叠情况")
    print("=" * 50)
    
    # 读取数据
    df = pd.read_parquet(parquet_file)
    
    print(f"📊 基本信息:")
    print(f"   总记录数: {len(df):,}")
    print(f"   总页数: {df['_page'].nunique()}")
    print(f"   日期范围: {df['ann_date'].min()} - {df['ann_date'].max()}")
    
    # 按页分组分析
    pages = sorted(df['_page'].unique())
    print(f"\n📄 按页分析:")
    
    for page in pages:
        page_data = df[df['_page'] == page]
        offset = page_data['_offset'].iloc[0]
        print(f"   第{page}页: {len(page_data):,}条记录, offset={offset}")
    
    # 重叠分析
    print(f"\n🔄 重叠分析:")
    
    for i in range(len(pages) - 1):
        current_page = pages[i]
        next_page = pages[i + 1]
        
        current_data = df[df['_page'] == current_page]
        next_data = df[df['_page'] == next_page]
        
        # 创建唯一键进行对比
        current_keys = set(current_data['ts_code'] + '_' + current_data['ann_date'].astype(str) + '_' + current_data['end_date'].astype(str))
        next_keys = set(next_data['ts_code'] + '_' + next_data['ann_date'].astype(str) + '_' + next_data['end_date'].astype(str))
        
        overlap = current_keys.intersection(next_keys)
        overlap_count = len(overlap)
        
        print(f"   第{current_page}页 与 第{next_page}页:")
        print(f"     重叠记录数: {overlap_count}")
        
        if overlap_count == 100:
            print(f"     ✅ 重叠数量正确！")
        elif overlap_count > 0:
            print(f"     ⚠️  重叠数量异常（期望100行）")
        else:
            print(f"     ❌ 无重叠，可能有数据丢失")
    
    # 整体去重分析
    print(f"\n📈 整体数据质量:")
    unique_df = df.drop_duplicates(subset=['ts_code', 'ann_date', 'end_date'])
    duplicate_count = len(df) - len(unique_df)
    
    print(f"   原始记录数: {len(df):,}")
    print(f"   去重后记录数: {len(unique_df):,}")
    print(f"   重复记录数: {duplicate_count:,}")
    
    # 期望重叠数
    expected_overlap = (len(pages) - 1) * 100
    print(f"   期望重叠数: {expected_overlap}")
    
    if duplicate_count == expected_overlap:
        print(f"   ✅ 重叠数量完全符合预期！")
    else:
        print(f"   ⚠️  重叠数量与预期不符")
    
    # 保存去重数据
    if duplicate_count > 0:
        output_file = Path(parquet_file).parent / f"{Path(parquet_file).stem}_deduplicated.parquet"
        unique_df.to_parquet(output_file, index=False)
        print(f"\n💾 去重数据已保存到: {output_file}")
    
    return {
        'total_records': len(df),
        'unique_records': len(unique_df),
        'duplicate_records': duplicate_count,
        'pages': len(pages),
        'expected_overlap': expected_overlap,
        'overlap_correct': duplicate_count == expected_overlap
    }

if __name__ == "__main__":
    # 检查测试结果文件
    test_files = [
        "pagination_test_result.parquet",
        "pagination_test_result_unique.parquet"
    ]
    
    for file_name in test_files:
        if Path(file_name).exists():
            print(f"\n{'='*60}")
            print(f"分析文件: {file_name}")
            print(f"{'='*60}")
            analyze_pagination_overlap(file_name)
        else:
            print(f"⚠️  文件不存在: {file_name}")
