import pandas as pd
import numpy as np
from difflib import SequenceMatcher
import re
from decimal import Decimal, InvalidOperation

class CSVComparator:
    def __init__(self, similarity_threshold=0.6, value_tolerance=1e-10):
        """
        初始化CSV对比器
        
        Args:
            similarity_threshold (float): 字段名相似度阈值，默认0.6
            value_tolerance (float): 数值比较容差，默认1e-10
        """
        self.similarity_threshold = similarity_threshold
        self.value_tolerance = value_tolerance
        self.field_mapping = {}
        
    def normalize_value(self, value):
        """
        标准化值，处理数值等效性
        """
        if pd.isna(value):
            return None
        
        # 转换为字符串并去除前后空格
        str_value = str(value).strip()
        
        # 尝试转换为数值
        try:
            # 尝试整数转换
            if str_value.isdigit() or (str_value.startswith('-') and str_value[1:].isdigit()):
                return int(str_value)
            
            # 尝试浮点数转换
            float_value = float(str_value)
            # 如果是整数值的浮点数，转换为整数
            if float_value.is_integer():
                return int(float_value)
            return float_value
            
        except (ValueError, OverflowError):
            # 如果不能转换为数值，返回标准化的字符串
            return str_value.lower()
    
    def values_are_equivalent(self, val1, val2):
        """
        判断两个值是否等效
        """
        norm_val1 = self.normalize_value(val1)
        norm_val2 = self.normalize_value(val2)
        
        # 如果都是None，认为相等
        if norm_val1 is None and norm_val2 is None:
            return True
        
        # 如果一个是None，另一个不是，认为不等
        if norm_val1 is None or norm_val2 is None:
            return False
        
        # 如果都是数值，使用容差比较
        if isinstance(norm_val1, (int, float)) and isinstance(norm_val2, (int, float)):
            return abs(norm_val1 - norm_val2) <= self.value_tolerance
        
        # 字符串比较
        return norm_val1 == norm_val2
    
    def compare_rows(self, row1, row2, fields):
        """
        比较两行数据，返回相同字段数和详细比较结果
        """
        total_fields = len(fields)
        identical_fields = 0
        field_comparisons = {}
        
        for field in fields:
            if field in row1 and field in row2:
                val1 = row1[field]
                val2 = row2[field]
                
                is_equivalent = self.values_are_equivalent(val1, val2)
                field_comparisons[field] = {
                    'val1': val1,
                    'val2': val2,
                    'equivalent': is_equivalent
                }
                
                if is_equivalent:
                    identical_fields += 1
        
        similarity_ratio = identical_fields / total_fields if total_fields > 0 else 0
        
        return {
            'total_fields': total_fields,
            'identical_fields': identical_fields,
            'similarity_ratio': similarity_ratio,
            'field_comparisons': field_comparisons,
            'is_fully_identical': similarity_ratio == 1.0
        }
        
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
    
    def compare_csvs(self, file1_path, file2_path, key_field1=None, key_field2=None, 
                     min_similarity_ratio=0.0, export_partial=True):
        """
        对比两个CSV文件，找出相同和部分相同的部分
        
        Args:
            file1_path (str): 第一个CSV文件路径
            file2_path (str): 第二个CSV文件路径
            key_field1 (str): 第一个文件的主键字段名（可选）
            key_field2 (str): 第二个文件的主键字段名（可选）
            min_similarity_ratio (float): 最小相似度比例，默认0.0（导出所有匹配的记录）
            export_partial (bool): 是否导出部分相同的记录，默认True
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
        
        # 如果指定了主键字段，使用主键进行精确匹配
        if key_field1 and key_field2:
            if key_field1 in df1.columns and key_field2 in df2.columns:
                # 使用主键合并找出相同记录
                df1_key = df1.set_index(key_field1)
                df2_key = df2.set_index(key_field2)
                
                # 重命名df2的列名
                rename_mapping = {field_mappings.get(col, col): col for col in df2_key.columns}
                df2_key = df2_key.rename(columns=rename_mapping)
                
                # 找到共同的键
                common_keys = df1_key.index.intersection(df2_key.index)
                
                # 获取相同键的数据
                df1_common = df1_key.loc[common_keys]
                df2_common = df2_key.loc[common_keys]
                
                # 逐行比较找出相同和部分相同的记录
                fully_identical_keys = []
                partially_identical_keys = []
                comparison_details = {}
                
                for key in common_keys:
                    row1 = df1_common.loc[key]
                    row2 = df2_common.loc[key]
                    
                    # 详细比较两行
                    comparison = self.compare_rows(row1, row2, common_fields)
                    comparison_details[key] = comparison
                    
                    if comparison['is_fully_identical']:
                        fully_identical_keys.append(key)
                    elif comparison['similarity_ratio'] >= min_similarity_ratio:
                        partially_identical_keys.append(key)
                
                # 按相似度排序部分相同的记录
                partially_identical_keys.sort(
                    key=lambda x: comparison_details[x]['similarity_ratio'], 
                    reverse=True
                )
                
                result = {
                    'field_mappings': field_mappings,
                    'common_keys': list(common_keys),
                    'fully_identical_keys': fully_identical_keys,
                    'partially_identical_keys': partially_identical_keys,
                    'comparison_details': comparison_details,
                    'file1_total': len(df1_key),
                    'file2_total': len(df2_key),
                    'common_records': len(common_keys),
                    'fully_identical_records': len(fully_identical_keys),
                    'partially_identical_records': len(partially_identical_keys),
                    'file1_common': df1_common,
                    'file2_common': df2_common,
                    'fully_identical_data': df1_common.loc[fully_identical_keys] if fully_identical_keys else pd.DataFrame(),
                    'partially_identical_data': df1_common.loc[partially_identical_keys] if partially_identical_keys else pd.DataFrame()
                }
                
                print(f"\n基于主键 '{key_field1}' <-> '{key_field2}' 的对比结果:")
                print(f"文件1总记录数: {len(df1_key)}")
                print(f"文件2总记录数: {len(df2_key)}")
                print(f"具有相同主键的记录数: {len(common_keys)}")
                print(f"完全相同的记录数: {len(fully_identical_keys)}")
                print(f"部分相同的记录数: {len(partially_identical_keys)}")
                
                return result
        
        # 如果没有指定主键，则进行整行数据对比
        fully_identical_indices = []
        partially_identical_indices = []
        comparison_details = {}
        
        # 比较每一行
        for i in range(len(df1_filtered)):
            row1 = df1_filtered.iloc[i]
            
            for j in range(len(df2_filtered)):
                row2 = df2_filtered.iloc[j]
                
                # 详细比较两行
                comparison = self.compare_rows(row1, row2, common_fields)
                
                if comparison['is_fully_identical']:
                    fully_identical_indices.append((i, j))
                    comparison_details[f"{i}-{j}"] = comparison
                elif comparison['similarity_ratio'] >= min_similarity_ratio:
                    partially_identical_indices.append((i, j))
                    comparison_details[f"{i}-{j}"] = comparison
        
        # 去重（一行可能匹配多行，选择最佳匹配）
        fully_identical_rows1 = list(set([idx[0] for idx in fully_identical_indices]))
        fully_identical_rows2 = list(set([idx[1] for idx in fully_identical_indices]))
        
        partially_identical_rows1 = list(set([idx[0] for idx in partially_identical_indices]))
        partially_identical_rows2 = list(set([idx[1] for idx in partially_identical_indices]))
        
        result = {
            'field_mappings': field_mappings,
            'common_fields': common_fields,
            'file1_shape': df1_filtered.shape,
            'file2_shape': df2_filtered.shape,
            'file1_data': df1_filtered,
            'file2_data': df2_filtered,
            'fully_identical_count': len(fully_identical_rows1),
            'partially_identical_count': len(partially_identical_rows1),
            'comparison_details': comparison_details,
            'fully_identical_data_file1': df1_filtered.iloc[fully_identical_rows1] if fully_identical_rows1 else pd.DataFrame(),
            'fully_identical_data_file2': df2_filtered.iloc[fully_identical_rows2] if fully_identical_rows2 else pd.DataFrame(),
            'partially_identical_data_file1': df1_filtered.iloc[partially_identical_rows1] if partially_identical_rows1 else pd.DataFrame(),
            'partially_identical_data_file2': df2_filtered.iloc[partially_identical_rows2] if partially_identical_rows2 else pd.DataFrame()
        }
        
        print(f"\n数据对比结果:")
        print(f"共同字段数: {len(common_fields)}")
        print(f"文件1数据形状: {df1_filtered.shape}")
        print(f"文件2数据形状: {df2_filtered.shape}")
        print(f"完全相同的行数: {len(fully_identical_rows1)}")
        print(f"部分相同的行数: {len(partially_identical_rows1)}")
        
        return result
    
    def find_field_value_matches(self, result, field_name):
        """
        分析指定字段中的相同值
        """
        if 'identical_data' in result:
            # 基于主键的结果
            if not result['identical_data'].empty and field_name in result['identical_data'].columns:
                values = result['identical_data'][field_name].dropna().unique()
                return {
                    'field_name': field_name,
                    'identical_values': list(values),
                    'count': len(values)
                }
        elif 'identical_data_file1' in result:
            # 基于整行的结果
            if not result['identical_data_file1'].empty and field_name in result['identical_data_file1'].columns:
                values = result['identical_data_file1'][field_name].dropna().unique()
                return {
                    'field_name': field_name,
                    'identical_values': list(values),
                    'count': len(values)
                }
        
        return None
    
    def get_detailed_comparison_report(self, result):
        """
        生成详细的对比报告
        """
        report = []
        
        # 字段映射报告
        report.append("=== 字段映射报告 ===")
        for f1, f2 in result['field_mappings'].items():
            similarity = self.calculate_similarity(f1, f2)
            report.append(f"'{f1}' -> '{f2}' (相似度: {similarity:.2f})")
        
        # 数据对比报告
        if 'identical_data' in result:
            report.append("\n=== 基于主键的数据对比报告 ===")
            report.append(f"文件1总记录数: {result['file1_total']}")
            report.append(f"文件2总记录数: {result['file2_total']}")
            report.append(f"相同主键记录数: {result['common_records']}")
            report.append(f"完全相同记录数: {result['identical_records']}")
            report.append(f"相同率: {result['identical_records']/result['common_records']*100:.2f}%" if result['common_records'] > 0 else "相同率: 0%")
            
            if result['identical_records'] > 0:
                report.append(f"\n完全相同的记录示例:")
                for i, (idx, row) in enumerate(result['identical_data'].head(3).iterrows()):
                    report.append(f"  记录 {idx}: {dict(row)}")
        
        elif 'identical_data_file1' in result:
            report.append("\n=== 基于整行数据的对比报告 ===")
            report.append(f"文件1总行数: {result['file1_shape'][0]}")
            report.append(f"文件2总行数: {result['file2_shape'][0]}")
            report.append(f"匹配字段数: {len(result['common_fields'])}")
            report.append(f"完全相同行数: {result['identical_rows_count']}")
            
            if result['identical_rows_count'] > 0:
                report.append(f"\n完全相同的行示例:")
                for i, (idx, row) in enumerate(result['identical_data_file1'].head(3).iterrows()):
                    report.append(f"  行 {idx}: {dict(row)}")
        
        return "\n".join(report)
    
    def export_comparison_result(self, result, output_path):
        """
        导出对比结果到Excel文件，重点导出相同的部分
        """
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            
            # 导出字段映射信息
            mapping_df = pd.DataFrame(list(result['field_mappings'].items()), 
                                    columns=['File1_Field', 'File2_Field'])
            mapping_df.to_excel(writer, sheet_name='字段映射', index=False)
            
            # 如果是基于主键的对比结果
            if 'identical_data' in result:
                # 导出完全相同的记录
                if not result['identical_data'].empty:
                    result['identical_data'].to_excel(writer, sheet_name='完全相同的记录', index=True)
                else:
                    pd.DataFrame({'提示': ['两个文件中没有找到完全相同的记录']}).to_excel(
                        writer, sheet_name='完全相同的记录', index=False)
                
                # 导出具有相同主键的记录（来自文件1）
                if not result['file1_common'].empty:
                    result['file1_common'].to_excel(writer, sheet_name='文件1_相同主键记录', index=True)
                
                # 导出具有相同主键的记录（来自文件2）
                if not result['file2_common'].empty:
                    result['file2_common'].to_excel(writer, sheet_name='文件2_相同主键记录', index=True)
                
                # 创建对比统计表
                stats_df = pd.DataFrame({
                    '统计项': ['文件1总记录数', '文件2总记录数', '相同主键记录数', '完全相同记录数'],
                    '数值': [result['file1_total'], result['file2_total'], 
                           result['common_records'], result['identical_records']]
                })
                stats_df.to_excel(writer, sheet_name='对比统计', index=False)
            
            # 如果是基于整行数据的对比结果
            elif 'identical_data_file1' in result:
                # 导出完全相同的行
                if not result['identical_data_file1'].empty:
                    result['identical_data_file1'].to_excel(writer, sheet_name='完全相同的行_文件1', index=True)
                    result['identical_data_file2'].to_excel(writer, sheet_name='完全相同的行_文件2', index=True)
                else:
                    pd.DataFrame({'提示': ['两个文件中没有找到完全相同的行']}).to_excel(
                        writer, sheet_name='完全相同的行', index=False)
                
                # 导出完整的匹配字段数据
                result['file1_data'].to_excel(writer, sheet_name='文件1_匹配字段数据', index=False)
                result['file2_data'].to_excel(writer, sheet_name='文件2_匹配字段数据', index=False)
                
                # 创建对比统计表
                stats_df = pd.DataFrame({
                    '统计项': ['文件1总行数', '文件2总行数', '匹配字段数', '完全相同行数'],
                    '数值': [result['file1_shape'][0], result['file2_shape'][0], 
                           len(result['common_fields']), result['identical_rows_count']]
                })
                stats_df.to_excel(writer, sheet_name='对比统计', index=False)
        
        print(f"对比结果已导出到: {output_path}")
        print(f"导出内容包括: 字段映射、完全相同的记录/行、对比统计等")

# 使用示例
def main():
    # 创建对比器实例
    comparator = CSVComparator(similarity_threshold=0.6)
    
    # 示例1: 基于主键的精确对比
    print("=== 示例1: 基于主键的对比 ===")
    result1 = comparator.compare_csvs(
        'file1.csv', 
        'file2.csv',
        key_field1='id',     # 文件1的主键字段
        key_field2='ID'      # 文件2的主键字段
    )
    
    if result1:
        # 生成详细报告
        report = comparator.get_detailed_comparison_report(result1)
        print(report)
        
        # 导出相同的部分
        comparator.export_comparison_result(result1, 'identical_records_by_key.xlsx')
    
    print("\n" + "="*50 + "\n")
    
    # 示例2: 基于整行数据的对比
    print("=== 示例2: 基于整行数据的对比 ===")
    result2 = comparator.compare_csvs('file1.csv', 'file2.csv')
    
    if result2:
        # 生成详细报告
        report = comparator.get_detailed_comparison_report(result2)
        print(report)
        
        # 分析特定字段的相同值
        if result2['common_fields']:
            field_name = result2['common_fields'][0]
            field_analysis = comparator.find_field_value_matches(result2, field_name)
            if field_analysis:
                print(f"\n字段 '{field_name}' 的相同值分析:")
                print(f"相同值数量: {field_analysis['count']}")
                print(f"相同值示例: {field_analysis['identical_values'][:10]}")
        
        # 导出相同的部分
        comparator.export_comparison_result(result2, 'identical_records_by_content.xlsx')

# 快速使用函数
def quick_compare(file1, file2, key1=None, key2=None, output_file=None):
    """
    快速对比两个CSV文件的相同部分
    
    Args:
        file1 (str): 第一个CSV文件路径
        file2 (str): 第二个CSV文件路径
        key1 (str): 第一个文件的主键字段名（可选）
        key2 (str): 第二个文件的主键字段名（可选）
        output_file (str): 输出Excel文件路径（可选）
    """
    comparator = CSVComparator(similarity_threshold=0.6)
    
    result = comparator.compare_csvs(file1, file2, key1, key2)
    
    if result:
        # 打印报告
        report = comparator.get_detailed_comparison_report(result)
        print(report)
        
        # 导出结果
        if output_file:
            comparator.export_comparison_result(result, output_file)
        else:
            output_file = 'comparison_identical_parts.xlsx'
            comparator.export_comparison_result(result, output_file)
        
        return result
    else:
        print("对比失败，请检查文件格式和内容")
        return None

if __name__ == "__main__":
    # 使用main函数运行完整示例
    main()
    
    # 或者使用快速对比函数
    # quick_compare('file1.csv', 'file2.csv', 'id', 'ID', 'output.xlsx')