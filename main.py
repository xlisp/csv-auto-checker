#!/usr/bin/env python3
"""
CSV 自动对比工具
基于主键的高性能CSV对比，生成HTML报告显示差异
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional, Set
import hashlib
import json
import random
from pathlib import Path
import time
from dataclasses import dataclass
from jinja2 import Template
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class CompareResult:
    """对比结果数据结构"""
    same_records: pd.DataFrame
    different_records: pd.DataFrame
    only_in_file1: pd.DataFrame
    only_in_file2: pd.DataFrame
    diff_details: List[Dict]
    stats: Dict

class CSVComparer:
    """CSV对比工具主类"""
    
    def __init__(self, primary_keys: List[str], sample_rate: float = 1.0):
        """
        初始化CSV对比工具
        
        Args:
            primary_keys: 主键列名列表，支持组合主键
            sample_rate: 采样率，用于大数据集的智能抽样对比 (0.0-1.0)
        """
        self.primary_keys = primary_keys
        self.sample_rate = sample_rate
        self.hash_cache = {}
        
    def load_csv(self, file_path: str, encoding: str = 'utf-8') -> pd.DataFrame:
        """加载CSV文件"""
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            logger.info(f"成功加载CSV文件: {file_path}, 行数: {len(df)}")
            return df
        except Exception as e:
            logger.error(f"加载CSV文件失败: {file_path}, 错误: {e}")
            raise
    
    def _create_composite_key(self, df: pd.DataFrame) -> pd.Series:
        """创建组合主键"""
        if len(self.primary_keys) == 1:
            return df[self.primary_keys[0]].astype(str)
        else:
            # 组合多个主键
            return df[self.primary_keys].astype(str).apply(
                lambda x: '|'.join(x.values), axis=1
            )
    
    def _calculate_row_hash(self, row: pd.Series) -> str:
        """计算行数据的哈希值，用于快速比较"""
        row_str = '|'.join(str(v) for v in row.values)
        return hashlib.md5(row_str.encode()).hexdigest()
    
    def _intelligent_sampling(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """智能抽样策略"""
        if self.sample_rate >= 1.0:
            return df1, df2
        
        # 获取共同的主键
        keys1 = set(self._create_composite_key(df1))
        keys2 = set(self._create_composite_key(df2))
        common_keys = keys1 & keys2
        
        if len(common_keys) == 0:
            logger.warning("没有找到共同的主键，使用随机抽样")
            sample_size1 = int(len(df1) * self.sample_rate)
            sample_size2 = int(len(df2) * self.sample_rate)
            return df1.sample(sample_size1), df2.sample(sample_size2)
        
        # 智能抽样：优先选择共同键的记录
        sample_size = int(len(common_keys) * self.sample_rate)
        sampled_keys = random.sample(list(common_keys), min(sample_size, len(common_keys)))
        
        df1_key = self._create_composite_key(df1)
        df2_key = self._create_composite_key(df2)
        
        df1_sampled = df1[df1_key.isin(sampled_keys)]
        df2_sampled = df2[df2_key.isin(sampled_keys)]
        
        logger.info(f"智能抽样完成，抽样后数据量: df1={len(df1_sampled)}, df2={len(df2_sampled)}")
        return df1_sampled, df2_sampled
    
    def compare_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                          file1_name: str = "File1", file2_name: str = "File2") -> CompareResult:
        """
        对比两个DataFrame
        
        Args:
            df1, df2: 要对比的DataFrame
            file1_name, file2_name: 文件名称，用于报告显示
            
        Returns:
            CompareResult: 对比结果
        """
        start_time = time.time()
        
        # 验证主键存在
        for key in self.primary_keys:
            if key not in df1.columns or key not in df2.columns:
                raise ValueError(f"主键 '{key}' 不存在于某个文件中")
        
        # 智能抽样
        df1_sample, df2_sample = self._intelligent_sampling(df1, df2)
        
        # 创建主键索引
        df1_key = self._create_composite_key(df1_sample)
        df2_key = self._create_composite_key(df2_sample)
        
        # 设置索引以提高查找性能
        df1_indexed = df1_sample.set_index(df1_key)
        df2_indexed = df2_sample.set_index(df2_key)
        
        # 找出各种情况的记录
        keys1 = set(df1_indexed.index)
        keys2 = set(df2_indexed.index)
        
        common_keys = keys1 & keys2
        only_in_file1_keys = keys1 - keys2
        only_in_file2_keys = keys2 - keys1
        
        # 只存在于文件1或文件2的记录
        only_in_file1 = df1_indexed.loc[list(only_in_file1_keys)] if only_in_file1_keys else pd.DataFrame()
        only_in_file2 = df2_indexed.loc[list(only_in_file2_keys)] if only_in_file2_keys else pd.DataFrame()
        
        # 比较共同记录
        same_records = []
        different_records = []
        diff_details = []
        
        for key in common_keys:
            row1 = df1_indexed.loc[key]
            row2 = df2_indexed.loc[key]
            
            # 使用哈希快速比较
            hash1 = self._calculate_row_hash(row1)
            hash2 = self._calculate_row_hash(row2)
            
            if hash1 == hash2:
                same_records.append(row1)
            else:
                different_records.append(row1)
                # 详细比较找出不同的字段
                diff_detail = self._compare_rows(row1, row2, key, file1_name, file2_name)
                diff_details.append(diff_detail)
        
        # 构建结果
        same_df = pd.DataFrame(same_records) if same_records else pd.DataFrame()
        different_df = pd.DataFrame(different_records) if different_records else pd.DataFrame()
        
        # 统计信息
        stats = {
            'total_records_file1': len(df1),
            'total_records_file2': len(df2),
            'sampled_records_file1': len(df1_sample),
            'sampled_records_file2': len(df2_sample),
            'same_records': len(same_df),
            'different_records': len(different_df),
            'only_in_file1': len(only_in_file1),
            'only_in_file2': len(only_in_file2),
            'processing_time': time.time() - start_time
        }
        
        logger.info(f"对比完成，耗时: {stats['processing_time']:.2f}秒")
        
        return CompareResult(
            same_records=same_df,
            different_records=different_df,
            only_in_file1=only_in_file1,
            only_in_file2=only_in_file2,
            diff_details=diff_details,
            stats=stats
        )
    
    def _compare_rows(self, row1: pd.Series, row2: pd.Series, key: str, 
                     file1_name: str, file2_name: str) -> Dict:
        """比较两行数据的详细差异"""
        differences = []
        
        for col in row1.index:
            if col in row2.index:
                val1, val2 = row1[col], row2[col]
                if pd.isna(val1) and pd.isna(val2):
                    continue
                elif val1 != val2:
                    differences.append({
                        'column': col,
                        f'{file1_name}_value': val1,
                        f'{file2_name}_value': val2
                    })
        
        return {
            'key': key,
            'differences': differences
        }
    
    def generate_html_report(self, result: CompareResult, output_path: str = "compare_report.html"):
        """生成HTML对比报告"""
        
        html_template = """
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>CSV对比报告</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; line-height: 1.6; }
                .header { background-color: #f4f4f4; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
                .stats { display: flex; justify-content: space-around; margin: 20px 0; }
                .stat-item { text-align: center; padding: 10px; background-color: #e9ecef; border-radius: 5px; }
                .stat-number { font-size: 24px; font-weight: bold; color: #007bff; }
                .section { margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }
                .section-title { font-size: 18px; font-weight: bold; margin-bottom: 10px; color: #333; }
                table { width: 100%; border-collapse: collapse; margin: 10px 0; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #f2f2f2; }
                .diff-row { background-color: #fff3cd; }
                .same-count { color: #28a745; }
                .diff-count { color: #dc3545; }
                .only-count { color: #fd7e14; }
                .diff-detail { margin: 10px 0; padding: 10px; background-color: #f8f9fa; border-radius: 5px; }
                .diff-field { margin: 5px 0; padding: 5px; background-color: #fff; border-left: 4px solid #dc3545; }
            </style>
        </head>
        <body>
            <div class="header">
                <h1>CSV文件对比报告</h1>
                <p>生成时间: {{ current_time }}</p>
                <p>处理时间: {{ stats.processing_time|round(2) }}秒</p>
            </div>
            
            <div class="stats">
                <div class="stat-item">
                    <div class="stat-number same-count">{{ stats.same_records }}</div>
                    <div>相同记录</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number diff-count">{{ stats.different_records }}</div>
                    <div>不同记录</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number only-count">{{ stats.only_in_file1 }}</div>
                    <div>仅在文件1</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number only-count">{{ stats.only_in_file2 }}</div>
                    <div>仅在文件2</div>
                </div>
            </div>
            
            {% if diff_details %}
            <div class="section">
                <div class="section-title">详细差异分析</div>
                {% for diff in diff_details[:50] %}
                <div class="diff-detail">
                    <strong>主键: {{ diff.key }}</strong>
                    {% for field_diff in diff.differences %}
                    <div class="diff-field">
                        <strong>{{ field_diff.column }}:</strong> 
                        文件1值: <code>{{ field_diff.File1_value }}</code> → 
                        文件2值: <code>{{ field_diff.File2_value }}</code>
                    </div>
                    {% endfor %}
                </div>
                {% endfor %}
                {% if diff_details|length > 50 %}
                <p><em>显示前50条差异记录...</em></p>
                {% endif %}
            </div>
            {% endif %}
            
            <div class="section">
                <div class="section-title">统计摘要</div>
                <table>
                    <tr><th>指标</th><th>数值</th></tr>
                    <tr><td>文件1总记录数</td><td>{{ stats.total_records_file1 }}</td></tr>
                    <tr><td>文件2总记录数</td><td>{{ stats.total_records_file2 }}</td></tr>
                    <tr><td>抽样记录数(文件1)</td><td>{{ stats.sampled_records_file1 }}</td></tr>
                    <tr><td>抽样记录数(文件2)</td><td>{{ stats.sampled_records_file2 }}</td></tr>
                    <tr><td>相同记录数</td><td class="same-count">{{ stats.same_records }}</td></tr>
                    <tr><td>不同记录数</td><td class="diff-count">{{ stats.different_records }}</td></tr>
                    <tr><td>仅在文件1的记录数</td><td class="only-count">{{ stats.only_in_file1 }}</td></tr>
                    <tr><td>仅在文件2的记录数</td><td class="only-count">{{ stats.only_in_file2 }}</td></tr>
                </table>
            </div>
        </body>
        </html>
        """
        
        template = Template(html_template)
        html_content = template.render(
            current_time=time.strftime("%Y-%m-%d %H:%M:%S"),
            stats=result.stats,
            diff_details=result.diff_details
        )
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML报告已生成: {output_path}")
        return output_path


def main():
    """主函数演示"""
    # 示例使用
    comparer = CSVComparer(
        primary_keys=['id'],  # 主键列
        sample_rate=0.1  # 抽样10%的数据进行对比
    )
    
    # 创建示例数据
    data1 = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 45],
        'city': ['NY', 'LA', 'Chicago', 'Boston', 'Seattle']
    }
    
    data2 = {
        'id': [1, 2, 3, 6, 7],
        'name': ['Alice', 'Bob', 'Charlie Brown', 'Frank', 'Grace'],
        'age': [25, 31, 35, 50, 28],
        'city': ['NY', 'LA', 'Chicago', 'Miami', 'Austin']
    }
    
    df1 = pd.DataFrame(data1)
    df2 = pd.DataFrame(data2)
    
    # 执行对比
    result = comparer.compare_dataframes(df1, df2, "示例文件1", "示例文件2")
    
    # 生成HTML报告
    comparer.generate_html_report(result)
    
    # 输出基本统计
    print("=== 对比结果统计 ===")
    for key, value in result.stats.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()

