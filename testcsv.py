import pandas as pd
import numpy as np
from difflib import SequenceMatcher
import re

class CSVComparator:
def **init**(self, similarity_threshold=0.6):
“””
初始化CSV对比器

```
    Args:
        similarity_threshold (float): 字段名相似度阈值，默认0.6
    """
    self.similarity_threshold = similarity_threshold
    self.field_mapping = {}
    
def normalize_field_name(self, field_name):
    """
    标准化字段名：去除空格、转小写、去除特殊字符
    """
    if pd.isna(field_name):
        return ""
    
    # 转为字符串并标准化
    field_name = str(field_name).strip().lower()
    # 去除特殊字符，只保留字母数字和下划线
    field_name = re.sub(r'[^a-z0-9_]', '', field_name)
    return field_name

def calculate_similarity(self, field1, field2):
    """
    计算两个字段名的相似度
    """
    norm1 = self.normalize_field_name(field1)
    norm2 = self.normalize_field_name(field2)
    
    if not norm1 or not norm2:
        return 0.0
        
    return SequenceMatcher(None, norm1, norm2).ratio()

def find_field_mappings(self, fields1, fields2):
    """
    自动找到两个CSV文件中相似的字段映射
    """
    mappings = {}
    used_fields2 = set()
    
    for field1 in fields1:
        best_match = None
        best_score = 0
        
        for field2 in fields2:
            if field2 in used_fields2:
                continue
                
            similarity = self.calculate_similarity(field1, field2)
            if similarity > best_score and similarity >= self.similarity_threshold:
                best_score = similarity
                best_match = field2
        
        if best_match:
            mappings[field1] = best_match
            used_fields2.add(best_match)
            
    return mappings

def load_and_preprocess_csv(self, file_path, encoding='utf-8'):
    """
    加载并预处理CSV文件
    """
    try:
        # 尝试不同的编码
        for enc in [encoding, 'utf-8', 'gbk', 'gb2312', 'utf-8-sig']:
            try:
                df = pd.read_csv(file_path, encoding=enc)
                print(f"成功读取文件 {file_path}，使用编码: {enc}")
                break
            except UnicodeDecodeError:
                continue
        else:
            raise ValueError(f"无法读取文件 {file_path}，请检查文件编码")
            
        # 清理列名
        df.columns = df.columns.str.strip()
        
        return df
        
    except Exception as e:
        print(f"读取文件 {file_path} 时出错: {e}")
        return None

def compare_csvs(self, file1_path, file2_path, key_field1=None, key_field2=None):
    """
    对比两个CSV文件
    
    Args:
        file1_path (str): 第一个CSV文件路径
        file2_path (str): 第二个CSV文件路径
        key_field1 (str): 第一个文件的主键字段名（可选）
        key_field2 (str): 第二个文件的主键字段名（可选）
    """
    # 加载CSV文件
    df1 = self.load_and_preprocess_csv(file1_path)
    df2 = self.load_and_preprocess_csv(file2_path)
    
    if df1 is None or df2 is None:
        return None
        
    print(f"文件1字段: {list(df1.columns)}")
    print(f"文件2字段: {list(df2.columns)}")
    
    # 自动找到字段映射
    field_mappings = self.find_field_mappings(df1.columns, df2.columns)
    self.field_mapping = field_mappings
    
    print(f"\n自动识别的字段映射:")
    for f1, f2 in field_mappings.items():
        similarity = self.calculate_similarity(f1, f2)
        print(f"  {f1} -> {f2} (相似度: {similarity:.2f})")
    
    if not field_mappings:
        print("未找到匹配的字段，请检查文件内容或调整相似度阈值")
        return None
    
    # 创建对比用的DataFrame
    common_fields = list(field_mappings.keys())
    df1_filtered = df1[common_fields].copy()
    df2_filtered = df2[[field_mappings[f] for f in common_fields]].copy()
    
    # 重命名df2的列名以匹配df1
    df2_filtered.columns = common_fields
    
    # 如果指定了主键字段，使用主键进行匹配
    if key_field1 and key_field2:
        if key_field1 in df1.columns and key_field2 in df2.columns:
            # 使用主键合并
            df1_key = df1.set_index(key_field1)
            df2_key = df2.set_index(key_field2)
            
            # 重命名df2的列名
            df2_key.columns = [field_mappings.get(col, col) for col in df2_key.columns]
            
            # 找到共同的键
            common_keys = df1_key.index.intersection(df2_key.index)
            
            result = {
                'common_records': len(common_keys),
                'file1_total': len(df1_key),
                'file2_total': len(df2_key),
                'common_data': {
                    'file1': df1_key.loc[common_keys],
                    'file2': df2_key.loc[common_keys]
                }
            }
            
            print(f"\n基于主键 '{key_field1}' <-> '{key_field2}' 的对比结果:")
            print(f"文件1总记录数: {len(df1_key)}")
            print(f"文件2总记录数: {len(df2_key)}")
            print(f"共同记录数: {len(common_keys)}")
            
            return result
    
    # 如果没有指定主键，则进行数据内容对比
    result = {
        'field_mappings': field_mappings,
        'common_fields': common_fields,
        'file1_shape': df1_filtered.shape,
        'file2_shape': df2_filtered.shape,
        'file1_data': df1_filtered,
        'file2_data': df2_filtered
    }
    
    print(f"\n数据对比结果:")
    print(f"共同字段数: {len(common_fields)}")
    print(f"文件1数据形状: {df1_filtered.shape}")
    print(f"文件2数据形状: {df2_filtered.shape}")
    
    return result

def find_common_values(self, result, field_name):
    """
    找到指定字段的共同值
    """
    if 'common_data' in result:
        # 基于主键的结果
        values1 = set(result['common_data']['file1'][field_name].dropna())
        values2 = set(result['common_data']['file2'][field_name].dropna())
    else:
        # 基于字段的结果
        values1 = set(result['file1_data'][field_name].dropna())
        values2 = set(result['file2_data'][field_name].dropna())
        
    common_values = values1.intersection(values2)
    return {
        'common_values': common_values,
        'file1_unique': values1 - values2,
        'file2_unique': values2 - values1,
        'total_common': len(common_values)
    }

def export_comparison_result(self, result, output_path):
    """
    导出对比结果到Excel文件
    """
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        if 'common_data' in result:
            # 基于主键的结果
            result['common_data']['file1'].to_excel(writer, sheet_name='File1_Common', index=True)
            result['common_data']['file2'].to_excel(writer, sheet_name='File2_Common', index=True)
        else:
            # 基于字段的结果
            result['file1_data'].to_excel(writer, sheet_name='File1_Data', index=False)
            result['file2_data'].to_excel(writer, sheet_name='File2_Data', index=False)
        
        # 字段映射信息
        mapping_df = pd.DataFrame(list(result['field_mappings'].items()), 
                                columns=['File1_Field', 'File2_Field'])
        mapping_df.to_excel(writer, sheet_name='Field_Mappings', index=False)
    
    print(f"对比结果已导出到: {output_path}")
```

# 使用示例

def main():
# 创建对比器实例
comparator = CSVComparator(similarity_threshold=0.6)

```
# 对比两个CSV文件
result = comparator.compare_csvs(
    'file1.csv', 
    'file2.csv',
    # key_field1='id',  # 可选：指定第一个文件的主键字段
    # key_field2='ID'   # 可选：指定第二个文件的主键字段
)

if result:
    # 查看特定字段的共同值
    if result['common_fields']:
        field_name = result['common_fields'][0]  # 选择第一个共同字段
        common_analysis = comparator.find_common_values(result, field_name)
        
        print(f"\n字段 '{field_name}' 的共同值分析:")
        print(f"共同值数量: {common_analysis['total_common']}")
        print(f"共同值示例: {list(common_analysis['common_values'])[:10]}")
    
    # 导出结果
    comparator.export_comparison_result(result, 'comparison_result.xlsx')
```

if **name** == “**main**”:
main()