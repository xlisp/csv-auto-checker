#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV文件组合主键查找器
递归查找能够唯一标识每条记录的最小字段组合
"""

import csv
import pandas as pd
from itertools import combinations
from pathlib import Path
import argparse
import sys


class CompositeKeyFinder:
    def __init__(self, csv_file_path, candidate_fields=None, max_key_length=None):
        """
        初始化组合主键查找器
        
        Args:
            csv_file_path (str): CSV文件路径
            candidate_fields (list): 候选字段列表，如果为None则使用所有字段
            max_key_length (int): 最大组合键长度，如果为None则搜索到所有字段
        """
        self.csv_file_path = Path(csv_file_path)
        self.df = None
        self.columns = []
        self.candidate_fields = candidate_fields
        self.max_key_length = max_key_length
        self.load_data()
    
    def load_data(self):
        """加载CSV数据"""
        try:
            # 尝试不同的编码格式
            encodings = ['utf-8', 'gbk', 'gb2312', 'latin-1']
            
            for encoding in encodings:
                try:
                    self.df = pd.read_csv(self.csv_file_path, encoding=encoding)
                    print(f"成功加载文件，使用编码: {encoding}")
                    break
                except UnicodeDecodeError:
                    continue
            
            if self.df is None:
                raise Exception("无法使用常见编码格式读取文件")
                
            self.columns = list(self.df.columns)
            
            # 处理候选字段
            if self.candidate_fields:
                # 验证候选字段是否存在
                invalid_fields = [f for f in self.candidate_fields if f not in self.columns]
                if invalid_fields:
                    print(f"警告: 以下字段不存在于CSV文件中: {invalid_fields}")
                
                # 只保留有效的候选字段
                self.candidate_fields = [f for f in self.candidate_fields if f in self.columns]
                if not self.candidate_fields:
                    print("警告: 没有有效的候选字段，将使用所有字段")
                    self.candidate_fields = self.columns
                else:
                    print(f"使用候选字段: {self.candidate_fields}")
            else:
                self.candidate_fields = self.columns
                print("使用所有字段作为候选字段")
            
            # 设置最大组合长度
            if self.max_key_length is None:
                self.max_key_length = len(self.candidate_fields)
            else:
                self.max_key_length = min(self.max_key_length, len(self.candidate_fields))
            
            print(f"数据形状: {self.df.shape}")
            print(f"所有列名: {self.columns}")
            print(f"候选字段: {self.candidate_fields}")
            print(f"最大组合长度: {self.max_key_length}")
            
        except Exception as e:
            print(f"加载数据失败: {e}")
            sys.exit(1)
    
    def check_uniqueness(self, column_combination):
        """
        检查给定的列组合是否能唯一标识所有记录
        
        Args:
            column_combination (tuple): 列名组合
            
        Returns:
            bool: 是否唯一
        """
        # 创建组合键
        if len(column_combination) == 1:
            combined_values = self.df[column_combination[0]].astype(str)
        else:
            combined_values = self.df[list(column_combination)].astype(str).apply(
                lambda x: '|'.join(x), axis=1
            )
        
        # 检查是否有重复值
        unique_count = combined_values.nunique()
        total_count = len(self.df)
        
        return unique_count == total_count
    
    def find_minimal_composite_keys(self):
        """
        递归查找最小的组合主键
        
        Returns:
            list: 找到的最小组合主键列表
        """
        print("\n开始查找组合主键...")
        print(f"搜索范围: {len(self.candidate_fields)} 个候选字段")
        print(f"最大组合长度: {self.max_key_length}")
        
        # 首先检查单个字段是否能作为主键
        print("\n1. 检查单字段主键:")
        single_key_candidates = []
        
        for col in self.candidate_fields:
            if self.check_uniqueness((col,)):
                single_key_candidates.append((col,))
                print(f"   ✓ 单字段主键: [{col}]")
        
        if single_key_candidates:
            print(f"\n找到 {len(single_key_candidates)} 个单字段主键")
            return single_key_candidates
        
        # 如果没有单字段主键，查找组合主键
        print("   未找到单字段主键，开始查找组合主键...")
        
        # 递归查找不同长度的组合
        for combo_length in range(2, self.max_key_length + 1):
            print(f"\n2. 检查 {combo_length} 字段组合:")
            
            combo_keys = []
            combo_count = 0
            total_combinations = len(list(combinations(self.candidate_fields, combo_length)))
            print(f"   总共需要检查 {total_combinations} 个组合")
            
            # 生成所有可能的组合
            for combination in combinations(self.candidate_fields, combo_length):
                combo_count += 1
                if combo_count % 100 == 0 or combo_count % max(1, total_combinations // 10) == 0:
                    print(f"   已检查 {combo_count}/{total_combinations} 个组合 ({combo_count/total_combinations*100:.1f}%)")
                
                if self.check_uniqueness(combination):
                    combo_keys.append(combination)
                    print(f"   ✓ 找到组合主键: {list(combination)}")
            
            if combo_keys:
                print(f"\n找到 {len(combo_keys)} 个 {combo_length} 字段组合主键")
                return combo_keys
        
        print("\n❌ 在指定范围内未找到任何有效的组合主键")
        return []
    
    def analyze_data_quality(self):
        """分析数据质量"""
        print("\n=== 数据质量分析 ===")
        print(f"总记录数: {len(self.df)}")
        print(f"总字段数: {len(self.columns)}")
        print(f"候选字段数: {len(self.candidate_fields)}")
        
        # 检查空值（只针对候选字段）
        print("\n候选字段空值统计:")
        null_counts = self.df[self.candidate_fields].isnull().sum()
        for col, count in null_counts.items():
            if count > 0:
                print(f"   {col}: {count} ({count/len(self.df)*100:.2f}%)")
            else:
                print(f"   {col}: 无空值")
        
        # 检查各候选字段的唯一值数量
        print("\n候选字段唯一值统计:")
        for col in self.candidate_fields:
            unique_count = self.df[col].nunique()
            duplicate_rate = (len(self.df) - unique_count) / len(self.df) * 100
            print(f"   {col}: {unique_count} 个唯一值 (重复率: {duplicate_rate:.2f}%)")
            
        # 显示候选字段的基本统计信息
        print("\n候选字段类型信息:")
        for col in self.candidate_fields:
            dtype = self.df[col].dtype
            sample_values = self.df[col].dropna().head(3).tolist()
            print(f"   {col}: {dtype} (示例: {sample_values})")
    
    def interactive_field_selection(self):
        """交互式选择候选字段"""
        print(f"\n可用字段 (共 {len(self.columns)} 个):")
        for i, col in enumerate(self.columns, 1):
            unique_count = self.df[col].nunique()
            null_count = self.df[col].isnull().sum()
            print(f"  {i:2d}. {col} (唯一值: {unique_count}, 空值: {null_count})")
        
        print("\n选择候选字段的方式:")
        print("1. 输入字段编号 (例如: 1,3,5-7)")
        print("2. 输入字段名称 (例如: name,id,date)")
        print("3. 直接回车使用所有字段")
        
        selection = input("\n请输入选择: ").strip()
        
        if not selection:
            return self.columns
        
        selected_fields = []
        
        # 尝试解析为数字编号
        if any(c.isdigit() for c in selection):
            try:
                parts = selection.split(',')
                for part in parts:
                    part = part.strip()
                    if '-' in part:
                        # 处理范围选择 (如 1-3)
                        start, end = map(int, part.split('-'))
                        for i in range(start, end + 1):
                            if 1 <= i <= len(self.columns):
                                selected_fields.append(self.columns[i-1])
                    else:
                        # 处理单个编号
                        i = int(part)
                        if 1 <= i <= len(self.columns):
                            selected_fields.append(self.columns[i-1])
            except ValueError:
                print("编号格式错误，使用所有字段")
                return self.columns
        else:
            # 尝试解析为字段名称
            field_names = [name.strip() for name in selection.split(',')]
            for name in field_names:
                if name in self.columns:
                    selected_fields.append(name)
                else:
                    print(f"警告: 字段 '{name}' 不存在")
        
        if not selected_fields:
            print("未选择有效字段，使用所有字段")
            return self.columns
        
        # 去重并保持顺序
        selected_fields = list(dict.fromkeys(selected_fields))
        print(f"已选择 {len(selected_fields)} 个字段: {selected_fields}")
        
        return selected_fields
    
    def interactive_max_length_selection(self):
        """交互式选择最大组合长度"""
        max_possible = len(self.candidate_fields)
        print(f"\n候选字段数量: {max_possible}")
        print("建议的最大组合长度:")
        print("  1-2: 查找简单主键")
        print("  3-4: 查找中等复杂度主键")
        print(f"  {max_possible}: 搜索所有可能组合（可能很慢）")
        
        while True:
            try:
                selection = input(f"\n请输入最大组合长度 (1-{max_possible}, 直接回车默认为 {min(4, max_possible)}): ").strip()
                
                if not selection:
                    return min(4, max_possible)
                
                max_length = int(selection)
                if 1 <= max_length <= max_possible:
                    return max_length
                else:
                    print(f"请输入 1 到 {max_possible} 之间的数字")
            except ValueError:
                print("请输入有效的数字")
    
    def generate_report(self, composite_keys):
        """生成详细报告"""
        print("\n" + "="*60)
        print("           组合主键查找报告")
        print("="*60)
        
        if not composite_keys:
            print("❌ 结果: 未找到有效的组合主键")
            print("\n可能的原因:")
            print("1. 数据中存在完全重复的记录")
            print("2. 所有字段组合都无法唯一标识记录")
            print("\n建议:")
            print("1. 检查并删除重复记录")
            print("2. 添加额外的标识字段（如ID、时间戳等）")
        else:
            print(f"✓ 结果: 找到 {len(composite_keys)} 个有效的组合主键")
            print("\n推荐的组合主键:")
            
            for i, key in enumerate(composite_keys, 1):
                print(f"\n{i}. 组合主键 (包含 {len(key)} 个字段):")
                for field in key:
                    print(f"   - {field}")
                
                # 显示该组合的示例数据
                print("   示例数据:")
                sample_data = self.df[list(key)].head(3)
                for idx, row in sample_data.iterrows():
                    values = " | ".join([str(v) for v in row.values])
                    print(f"     {values}")
        
        print("\n" + "="*60)


def find_csv_files_recursively(directory):
    """递归查找目录下的所有CSV文件"""
    directory = Path(directory)
    csv_files = []
    
    for file_path in directory.rglob("*.csv"):
        csv_files.append(file_path)
    
    return csv_files


def main():
    parser = argparse.ArgumentParser(description='CSV文件组合主键查找器')
    parser.add_argument('path', help='CSV文件路径或包含CSV文件的目录路径')
    parser.add_argument('--recursive', '-r', action='store_true', 
                       help='递归搜索目录下的所有CSV文件')
    parser.add_argument('--analyze', '-a', action='store_true',
                       help='同时进行数据质量分析')
    parser.add_argument('--fields', '-f', type=str,
                       help='候选字段列表，用逗号分隔 (例如: id,name,date)')
    parser.add_argument('--max-length', '-m', type=int,
                       help='最大组合键长度')
    parser.add_argument('--interactive', '-i', action='store_true',
                       help='交互式选择字段和参数')
    
    args = parser.parse_args()
    
    input_path = Path(args.path)
    
    # 确定要处理的CSV文件列表
    if input_path.is_file() and input_path.suffix.lower() == '.csv':
        csv_files = [input_path]
    elif input_path.is_dir():
        if args.recursive:
            csv_files = find_csv_files_recursively(input_path)
        else:
            csv_files = list(input_path.glob("*.csv"))
    else:
        print(f"错误: {input_path} 不是有效的CSV文件或目录")
        sys.exit(1)
    
    if not csv_files:
        print("未找到CSV文件")
        sys.exit(1)
    
    print(f"找到 {len(csv_files)} 个CSV文件")
    
    # 解析候选字段
    candidate_fields = None
    if args.fields:
        candidate_fields = [f.strip() for f in args.fields.split(',')]
    
    # 处理每个CSV文件
    for i, csv_file in enumerate(csv_files, 1):
        print(f"\n{'='*80}")
        print(f"处理文件 {i}/{len(csv_files)}: {csv_file}")
        print('='*80)
        
        try:
            # 根据参数创建查找器
            if args.interactive:
                # 交互式模式下，先创建临时查找器获取字段信息
                temp_finder = CompositeKeyFinder(csv_file, None, None)
                
                print("\n=== 交互式配置 ===")
                
                # 选择候选字段
                if not candidate_fields:
                    selected_fields = temp_finder.interactive_field_selection()
                else:
                    selected_fields = candidate_fields
                
                # 选择最大组合长度
                if not args.max_length:
                    # 更新候选字段后重新计算最大长度
                    temp_finder.candidate_fields = selected_fields
                    max_length = temp_finder.interactive_max_length_selection()
                else:
                    max_length = args.max_length
                
                # 创建配置好的查找器
                finder = CompositeKeyFinder(csv_file, selected_fields, max_length)
            else:
                # 非交互式模式
                finder = CompositeKeyFinder(csv_file, candidate_fields, args.max_length)
            
            if args.analyze:
                finder.analyze_data_quality()
            
            composite_keys = finder.find_minimal_composite_keys()
            finder.generate_report(composite_keys)
            
        except Exception as e:
            print(f"处理文件 {csv_file} 时出错: {e}")


if __name__ == "__main__":
    # 如果直接运行脚本，提供简单的交互式模式
    if len(sys.argv) == 1:
        print("CSV文件组合主键查找器")
        print("-" * 40)
        
        file_path = input("请输入CSV文件路径: ").strip()
        if not file_path:
            print("未提供文件路径")
            sys.exit(1)
        
        try:
            # 先创建基本的查找器来获取字段信息
            temp_finder = CompositeKeyFinder(file_path, None, None)
            
            # 交互式选择候选字段
            print("\n=== 字段选择 ===")
            candidate_fields = temp_finder.interactive_field_selection()
            
            # 交互式选择最大组合长度
            print("\n=== 参数设置 ===")
            temp_finder.candidate_fields = candidate_fields
            max_length = temp_finder.interactive_max_length_selection()
            
            # 创建配置好的查找器
            finder = CompositeKeyFinder(file_path, candidate_fields, max_length)
            
            # 询问是否进行数据质量分析
            analyze = input("\n是否进行数据质量分析? (y/N): ").strip().lower() == 'y'
            if analyze:
                finder.analyze_data_quality()
            
            composite_keys = finder.find_minimal_composite_keys()
            finder.generate_report(composite_keys)
            
        except Exception as e:
            print(f"错误: {e}")
    else:
        main()

