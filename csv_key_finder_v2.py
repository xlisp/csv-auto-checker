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
    def __init__(self, csv_file_path):
        """
        初始化组合主键查找器
        
        Args:
            csv_file_path (str): CSV文件路径
        """
        self.csv_file_path = Path(csv_file_path)
        self.df = None
        self.columns = []
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
            print(f"数据形状: {self.df.shape}")
            print(f"列名: {self.columns}")
            
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
        
        # 首先检查单个字段是否能作为主键
        print("\n1. 检查单字段主键:")
        single_key_candidates = []
        
        for col in self.columns:
            if self.check_uniqueness((col,)):
                single_key_candidates.append((col,))
                print(f"   ✓ 单字段主键: [{col}]")
        
        if single_key_candidates:
            print(f"\n找到 {len(single_key_candidates)} 个单字段主键")
            return single_key_candidates
        
        # 如果没有单字段主键，查找组合主键
        print("   未找到单字段主键，开始查找组合主键...")
        
        # 递归查找不同长度的组合
        for combo_length in range(2, len(self.columns) + 1):
            print(f"\n2. 检查 {combo_length} 字段组合:")
            
            combo_keys = []
            combo_count = 0
            
            # 生成所有可能的组合
            for combination in combinations(self.columns, combo_length):
                combo_count += 1
                if combo_count % 100 == 0:
                    print(f"   已检查 {combo_count} 个组合...")
                
                if self.check_uniqueness(combination):
                    combo_keys.append(combination)
                    print(f"   ✓ 找到组合主键: {list(combination)}")
            
            if combo_keys:
                print(f"\n找到 {len(combo_keys)} 个 {combo_length} 字段组合主键")
                return combo_keys
        
        print("\n❌ 未找到任何有效的组合主键")
        return []
    
    def analyze_data_quality(self):
        """分析数据质量"""
        print("\n=== 数据质量分析 ===")
        print(f"总记录数: {len(self.df)}")
        print(f"总字段数: {len(self.columns)}")
        
        # 检查空值
        print("\n空值统计:")
        null_counts = self.df.isnull().sum()
        for col, count in null_counts.items():
            if count > 0:
                print(f"   {col}: {count} ({count/len(self.df)*100:.2f}%)")
        
        # 检查各字段的唯一值数量
        print("\n各字段唯一值统计:")
        for col in self.columns:
            unique_count = self.df[col].nunique()
            duplicate_rate = (len(self.df) - unique_count) / len(self.df) * 100
            print(f"   {col}: {unique_count} 个唯一值 (重复率: {duplicate_rate:.2f}%)")
    
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
            finder = CompositeKeyFinder(csv_file, candidate_fields, args.max_length)
            
            # 交互式模式
            if args.interactive:
                print("\n=== 交互式配置 ===")
                
                # 选择候选字段
                if not candidate_fields:
                    finder.candidate_fields = finder.interactive_field_selection()
                
                # 选择最大组合长度
                if not args.max_length:
                    finder.max_key_length = finder.interactive_max_length_selection()
            
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
            temp_finder = CompositeKeyFinder(file_path)
            
            # 交互式选择候选字段
            print("\n=== 字段选择 ===")
            candidate_fields = temp_finder.interactive_field_selection()
            
            # 交互式选择最大组合长度
            print("\n=== 参数设置 ===")
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

