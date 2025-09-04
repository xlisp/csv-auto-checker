#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import csv
import sys
import os
import argparse
from pathlib import Path

def run_ag_search(keyword, search_path='.'):
    """
    使用ag命令搜索关键字，返回匹配的文件路径列表
    """
    try:
        # 使用ag命令搜索，只返回包含关键字的CSV文件
        cmd = ['ag', '-l', '--csv', keyword, search_path]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # 过滤出CSV文件
        csv_files = []
        for line in result.stdout.strip().split('\n'):
            if line.strip() and line.endswith('.csv'):
                csv_files.append(line.strip())
        
        return csv_files
    except subprocess.CalledProcessError as e:
        print(f"ag命令执行失败: {e}")
        return []
    except FileNotFoundError:
        print("错误: 找不到ag命令，请确保已安装the_silver_searcher")
        return []

def read_csv_file(filepath):
    """
    读取CSV文件，返回字段名和数据
    """
    try:
        with open(filepath, 'r', encoding='utf-8', newline='') as file:
            # 检测CSV方言
            sample = file.read(1024)
            file.seek(0)
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample)
            
            # 读取CSV数据
            reader = csv.DictReader(file, dialect=dialect)
            data = list(reader)
            fieldnames = reader.fieldnames
            
            return fieldnames, data
    except UnicodeDecodeError:
        # 如果UTF-8失败，尝试其他编码
        try:
            with open(filepath, 'r', encoding='gbk', newline='') as file:
                reader = csv.DictReader(file)
                data = list(reader)
                fieldnames = reader.fieldnames
                return fieldnames, data
        except Exception as e:
            print(f"读取文件 {filepath} 失败: {e}")
            return None, None
    except Exception as e:
        print(f"读取文件 {filepath} 失败: {e}")
        return None, None

def search_matching_records(data, search_criteria):
    """
    根据搜索条件匹配记录
    search_criteria: 字典格式，如 {'name': 'steve', 'age': '30'}
    """
    matching_records = []
    
    for record in data:
        match = True
        for field, value in search_criteria.items():
            # 不区分大小写的匹配
            if field.lower() not in [k.lower() for k in record.keys()]:
                match = False
                break
            
            # 找到对应的字段名（保持原始大小写）
            actual_field = None
            for k in record.keys():
                if k.lower() == field.lower():
                    actual_field = k
                    break
            
            if actual_field and record[actual_field].lower() != str(value).lower():
                match = False
                break
        
        if match:
            matching_records.append(record)
    
    return matching_records

def print_results(filepath, fieldnames, matching_records):
    """
    打印搜索结果
    """
    print(f"\n文件: {filepath}")
    print(f"字段名: {', '.join(fieldnames)}")
    print(f"匹配记录数: {len(matching_records)}")
    
    if matching_records:
        print("\n匹配的记录:")
        for i, record in enumerate(matching_records, 1):
            print(f"  记录 {i}:")
            for field, value in record.items():
                print(f"    {field}: {value}")

def parse_search_criteria(criteria_str):
    """
    解析搜索条件字符串
    例如: "name:steve,age:30" 或 "name=steve,age=30"
    """
    criteria = {}
    if not criteria_str:
        return criteria
    
    pairs = criteria_str.split(',')
    for pair in pairs:
        if ':' in pair:
            key, value = pair.split(':', 1)
        elif '=' in pair:
            key, value = pair.split('=', 1)
        else:
            continue
        
        criteria[key.strip()] = value.strip()
    
    return criteria

def main():
    parser = argparse.ArgumentParser(
        description='使用ag命令搜索CSV文件并提取匹配数据，支持多种分隔符(逗号、管道符、分号等)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用示例:
  python %(prog)s steve "name:steve,age:30"
  python %(prog)s john "name=john" --path /data
  python %(prog)s keyword "field1:value1,field2:value2"
  python %(prog)s shipped --list-files  # 只列出包含关键字的文件
        '''
    )
    
    parser.add_argument('keyword', help='要搜索的关键字')
    parser.add_argument('criteria', nargs='?', default='', 
                       help='搜索条件 (格式: field1:value1,field2:value2)')
    parser.add_argument('--path', '-p', default='.', 
                       help='搜索路径 (默认: 当前目录)')
    parser.add_argument('--output', '-o', help='输出结果到文件')
    parser.add_argument('--list-files', '-l', action='store_true',
                       help='只列出包含关键字的CSV文件，不读取内容')
    parser.add_argument('--debug', '-d', action='store_true',
                       help='显示调试信息')
    
    args = parser.parse_args()
    
    # 检查搜索路径
    if not os.path.exists(args.path):
        print(f"错误: 指定的路径不存在: {args.path}")
        return
    
    if args.debug:
        print(f"搜索路径: {os.path.abspath(args.path)}")
        print(f"关键字: '{args.keyword}'")
    
    # 搜索CSV文件
    print(f"正在搜索包含关键字 '{args.keyword}' 的CSV文件...")
    csv_files = run_ag_search(args.keyword, args.path)
    
    if not csv_files:
        print("未找到匹配的CSV文件")
        
        # 提供一些调试信息
        if args.debug:
            print(f"\n调试信息:")
            print(f"搜索路径: {os.path.abspath(args.path)}")
            
            # 检查目录中是否有CSV文件
            try:
                all_csv = []
                for root, dirs, files in os.walk(args.path):
                    for file in files:
                        if file.endswith('.csv'):
                            all_csv.append(os.path.join(root, file))
                
                print(f"目录中找到的CSV文件总数: {len(all_csv)}")
                if all_csv:
                    print("CSV文件列表:")
                    for csv_file in all_csv[:5]:  # 只显示前5个
                        print(f"  {csv_file}")
                    if len(all_csv) > 5:
                        print(f"  ... 还有 {len(all_csv) - 5} 个文件")
            except Exception as e:
                print(f"检查CSV文件时出错: {e}")
        
        return
    
    print(f"找到 {len(csv_files)} 个CSV文件:")
    for csv_file in csv_files:
        print(f"  {csv_file}")
    
    # 如果只是列出文件，就到此为止
    if args.list_files:
        return
    
    # 解析搜索条件
    search_criteria = parse_search_criteria(args.criteria)
    if search_criteria:
        print(f"搜索条件: {search_criteria}")
    
    # 处理每个CSV文件
    all_results = []
    for csv_file in csv_files:
        fieldnames, data = read_csv_file(csv_file)
        
        if fieldnames is None or data is None:
            continue
        
        # 如果没有指定搜索条件，显示所有数据
        if not search_criteria:
            matching_records = data
        else:
            matching_records = search_matching_records(data, search_criteria)
        
        print_results(csv_file, fieldnames, matching_records)
        
        # 收集结果用于可能的输出
        for record in matching_records:
            result_record = {'file': csv_file}
            result_record.update(record)
            all_results.append(result_record)
    
    # 输出到文件
    if args.output and all_results:
        try:
            with open(args.output, 'w', encoding='utf-8', newline='') as f:
                if all_results:
                    fieldnames = list(all_results[0].keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(all_results)
            print(f"\n结果已保存到: {args.output}")
        except Exception as e:
            print(f"保存文件失败: {e}")
    
    print(f"\n总共找到 {len(all_results)} 条匹配记录")

if __name__ == '__main__':
    main()

