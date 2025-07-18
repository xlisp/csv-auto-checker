import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional

def compare_csv_files(file1_path: str, 
                     file2_path: str, 
                     field_mapping: Dict[str, str],
                     key_fields: List[str],
                     compare_fields: Optional[List[str]] = None,
                     output_file: Optional[str] = None) -> pd.DataFrame:
    """
    对比两个CSV文件的共同部分
    
    参数:
    file1_path: 第一个CSV文件路径
    file2_path: 第二个CSV文件路径
    field_mapping: 字段映射字典，格式 {'file1_field': 'file2_field'}
    key_fields: 用于匹配的关键字段列表（基于file1的字段名）
    compare_fields: 需要对比的字段列表（基于file1的字段名），如果为None则对比所有映射字段
    output_file: 输出文件路径（可选）
    
    返回:
    包含对比结果的DataFrame
    """
    
    # 读取CSV文件
    try:
        df1 = pd.read_csv(file1_path)
        df2 = pd.read_csv(file2_path)
        print(f"文件1: {len(df1)} 行数据")
        print(f"文件2: {len(df2)} 行数据")
    except Exception as e:
        print(f"读取文件错误: {e}")
        return None
    
    # 验证字段映射
    missing_fields_1 = [field for field in field_mapping.keys() if field not in df1.columns]
    missing_fields_2 = [field for field in field_mapping.values() if field not in df2.columns]
    
    if missing_fields_1:
        print(f"文件1中缺少字段: {missing_fields_1}")
        return None
    if missing_fields_2:
        print(f"文件2中缺少字段: {missing_fields_2}")
        return None
    
    # 验证关键字段
    missing_key_fields = [field for field in key_fields if field not in df1.columns]
    if missing_key_fields:
        print(f"文件1中缺少关键字段: {missing_key_fields}")
        return None
    
    # 重命名file2的字段以匹配file1
    df2_renamed = df2.copy()
    reverse_mapping = {v: k for k, v in field_mapping.items()}
    df2_renamed = df2_renamed.rename(columns=reverse_mapping)
    
    # 确定要对比的字段
    if compare_fields is None:
        compare_fields = list(field_mapping.keys())
    
    # 验证对比字段
    missing_compare_fields = [field for field in compare_fields if field not in df1.columns]
    if missing_compare_fields:
        print(f"文件1中缺少对比字段: {missing_compare_fields}")
        return None
    
    # 基于关键字段进行内连接，找到共同部分
    merged_df = pd.merge(df1, df2_renamed, on=key_fields, how='inner', suffixes=('_file1', '_file2'))
    print(f"共同记录数: {len(merged_df)}")
    
    if len(merged_df) == 0:
        print("没有找到共同记录")
        return pd.DataFrame()
    
    # 创建对比结果
    result_data = []
    
    for idx, row in merged_df.iterrows():
        # 添加关键字段信息
        record = {}
        for key_field in key_fields:
            record[key_field] = row[key_field]
        
        # 对比每个字段
        has_difference = False
        for field in compare_fields:
            file1_col = f"{field}_file1" if f"{field}_file1" in merged_df.columns else field
            file2_col = f"{field}_file2" if f"{field}_file2" in merged_df.columns else field
            
            val1 = row[file1_col]
            val2 = row[file2_col]
            
            # 处理NaN值
            if pd.isna(val1) and pd.isna(val2):
                is_different = False
            elif pd.isna(val1) or pd.isna(val2):
                is_different = True
            else:
                is_different = val1 != val2
            
            record[f"{field}_file1"] = val1
            record[f"{field}_file2"] = val2
            record[f"{field}_diff"] = is_different
            
            if is_different:
                has_difference = True
        
        record['has_difference'] = has_difference
        result_data.append(record)
    
    result_df = pd.DataFrame(result_data)
    
    # 统计信息
    total_records = len(result_df)
    different_records = len(result_df[result_df['has_difference'] == True])
    same_records = total_records - different_records
    
    print(f"\n=== 对比结果统计 ===")
    print(f"总共同记录数: {total_records}")
    print(f"完全相同记录数: {same_records}")
    print(f"有差异记录数: {different_records}")
    print(f"相同率: {same_records/total_records*100:.2f}%")
    
    # 字段级别的差异统计
    print(f"\n=== 字段差异统计 ===")
    for field in compare_fields:
        if f"{field}_diff" in result_df.columns:
            diff_count = result_df[f"{field}_diff"].sum()
            print(f"{field}: {diff_count}/{total_records} ({diff_count/total_records*100:.2f}%)")
    
    # 保存结果
    if output_file:
        result_df.to_csv(output_file, index=False)
        print(f"\n结果已保存到: {output_file}")
    
    return result_df

# 使用示例
if __name__ == "__main__":
    # 示例配置
    file1_path = "file1.csv"
    file2_path = "file2.csv"
    
    # 字段映射：左边是file1的字段名，右边是file2的字段名
    field_mapping = {
        'user_id': 'id',
        'user_name': 'name',
        'user_email': 'email',
        'user_age': 'age',
        'user_city': 'city'
    }
    
    # 关键字段（用于匹配记录）
    key_fields = ['user_id']
    
    # 需要对比的字段（如果不指定，则对比所有映射字段）
    compare_fields = ['user_name', 'user_email', 'user_age', 'user_city']
    
    # 执行对比
    result = compare_csv_files(
        file1_path=file1_path,
        file2_path=file2_path,
        field_mapping=field_mapping,
        key_fields=key_fields,
        compare_fields=compare_fields,
        output_file="comparison_result.csv"
    )
    
    # 显示有差异的记录
    if result is not None and len(result) > 0:
        print("\n=== 有差异的记录示例 ===")
        different_records = result[result['has_difference'] == True]
        if len(different_records) > 0:
            print(different_records.head())
        else:
            print("所有记录都相同！")

# 高级功能：批量对比多个字段映射
def batch_compare_multiple_mappings(file1_path: str, 
                                  file2_path: str, 
                                  mapping_configs: List[Dict],
                                  output_dir: str = "./"):
    """
    批量对比多个字段映射配置
    
    mapping_configs: 包含多个配置的列表，每个配置包含：
    - name: 配置名称
    - field_mapping: 字段映射
    - key_fields: 关键字段
    - compare_fields: 对比字段
    """
    
    results = {}
    
    for config in mapping_configs:
        print(f"\n{'='*50}")
        print(f"执行配置: {config['name']}")
        print(f"{'='*50}")
        
        output_file = f"{output_dir}/{config['name']}_comparison.csv"
        
        result = compare_csv_files(
            file1_path=file1_path,
            file2_path=file2_path,
            field_mapping=config['field_mapping'],
            key_fields=config['key_fields'],
            compare_fields=config.get('compare_fields'),
            output_file=output_file
        )
        
        results[config['name']] = result
    
    return results