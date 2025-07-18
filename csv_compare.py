#!/usr/bin/env python3
"""
CSV 自动对比工具
基于主键的高性能CSV对比，生成HTML报告显示差异
"""

import pandas as pd
import numpy as np
import hashlib
import json
import random
from typing import List, Dict, Any, Tuple, Optional
from pathlib import Path
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CSVComparator:
    """高性能CSV对比工具"""
    
    def __init__(self, primary_keys: List[str], sample_rate: float = 0.1):
        """
        初始化对比器
        
        Args:
            primary_keys: 主键列名列表（支持组合主键）
            sample_rate: 智能抽样率（0-1之间）
        """
        self.primary_keys = primary_keys
        self.sample_rate = sample_rate
        self.comparison_results = {}
        
    def load_csv_chunked(self, file_path: str, chunk_size: int = 10000) -> pd.DataFrame:
        """
        分块加载大CSV文件
        
        Args:
            file_path: CSV文件路径
            chunk_size: 分块大小
            
        Returns:
            合并后的DataFrame
        """
        logger.info(f"开始加载CSV文件: {file_path}")
        chunks = []
        
        try:
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                chunks.append(chunk)
                
            df = pd.concat(chunks, ignore_index=True)
            logger.info(f"成功加载 {len(df)} 行数据")
            return df
            
        except Exception as e:
            logger.error(f"加载CSV文件失败: {e}")
            raise
    
    def create_composite_key(self, df: pd.DataFrame) -> pd.Series:
        """
        创建组合主键
        
        Args:
            df: 数据框
            
        Returns:
            组合主键Series
        """
        if len(self.primary_keys) == 1:
            return df[self.primary_keys[0]].astype(str)
        else:
            # 组合多个主键，确保所有值都转换为字符串
            key_parts = []
            for key in self.primary_keys:
                key_parts.append(df[key].astype(str))
            return pd.Series(['|'.join(parts) for parts in zip(*key_parts)], index=df.index)
    
    def intelligent_sampling(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        智能抽样策略
        
        Args:
            df1, df2: 待对比的数据框
            
        Returns:
            抽样后的数据框
        """
        # 创建主键
        key1 = self.create_composite_key(df1)
        key2 = self.create_composite_key(df2)
        
        logger.info(f"文件1中的主键示例: {key1.head(3).tolist()}")
        logger.info(f"文件2中的主键示例: {key2.head(3).tolist()}")
        
        # 找到共同的主键
        common_keys = set(key1) & set(key2)
        
        logger.info(f"找到 {len(common_keys)} 个共同主键")
        
        if len(common_keys) == 0:
            logger.warning("未找到共同的主键，使用随机抽样")
            sample_size = int(min(len(df1), len(df2)) * self.sample_rate)
            return df1.sample(n=sample_size), df2.sample(n=sample_size)
        
        # 如果抽样率为1.0或共同主键数较少，直接返回所有共同主键的数据
        if self.sample_rate >= 1.0 or len(common_keys) <= 100:
            sampled_keys = list(common_keys)
        else:
            # 对共同主键进行抽样
            sample_size = int(len(common_keys) * self.sample_rate)
            sampled_keys = random.sample(list(common_keys), sample_size)
        
        # 筛选数据
        df1_sampled = df1[key1.isin(sampled_keys)]
        df2_sampled = df2[key2.isin(sampled_keys)]
        
        logger.info(f"智能抽样完成: {len(sampled_keys)} 个主键, {len(df1_sampled)} + {len(df2_sampled)} 行数据")
        return df1_sampled, df2_sampled
    
    def parallel_compare_chunks(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                               chunk_size: int = 1000) -> Dict[str, Any]:
        """
        并行对比数据块
        
        Args:
            df1, df2: 待对比的数据框
            chunk_size: 分块大小
            
        Returns:
            对比结果字典
        """
        # 创建主键索引
        key1 = self.create_composite_key(df1)
        key2 = self.create_composite_key(df2)
        
        df1_indexed = df1.set_index(key1)
        df2_indexed = df2.set_index(key2)
        
        # 找到共同主键
        common_keys = list(set(df1_indexed.index) & set(df2_indexed.index))
        
        results = {
            'total_keys': len(set(df1_indexed.index) | set(df2_indexed.index)),
            'common_keys': len(common_keys),
            'only_in_df1': len(set(df1_indexed.index) - set(df2_indexed.index)),
            'only_in_df2': len(set(df2_indexed.index) - set(df1_indexed.index)),
            'differences': [],
            'identical_rows': 0
        }
        
        # 并行处理共同主键
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            
            for i in range(0, len(common_keys), chunk_size):
                chunk_keys = common_keys[i:i + chunk_size]
                future = executor.submit(
                    self._compare_chunk, 
                    df1_indexed, df2_indexed, chunk_keys
                )
                futures.append(future)
            
            # 收集结果
            for future in as_completed(futures):
                chunk_result = future.result()
                results['differences'].extend(chunk_result['differences'])
                results['identical_rows'] += chunk_result['identical_rows']
        
        logger.info(f"对比完成: {results['common_keys']} 个共同主键")
        return results
    
    def _compare_chunk(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                      keys: List[str]) -> Dict[str, Any]:
        """
        对比数据块
        
        Args:
            df1, df2: 已索引的数据框
            keys: 要对比的主键列表
            
        Returns:
            块对比结果
        """
        differences = []
        identical_rows = 0
        
        for key in keys:
            try:
                row1 = df1.loc[key]
                row2 = df2.loc[key]
                
                # 处理Series和DataFrame的情况
                if isinstance(row1, pd.DataFrame):
                    row1 = row1.iloc[0]
                if isinstance(row2, pd.DataFrame):
                    row2 = row2.iloc[0]
                
                # 对比每个字段
                diff_fields = []
                for col in df1.columns:
                    if col in df2.columns:
                        val1 = row1[col] if not pd.isna(row1[col]) else ""
                        val2 = row2[col] if not pd.isna(row2[col]) else ""
                        
                        if str(val1) != str(val2):
                            diff_fields.append({
                                'field': col,
                                'value1': val1,
                                'value2': val2
                            })
                
                if diff_fields:
                    differences.append({
                        'key': key,
                        'differences': diff_fields
                    })
                else:
                    identical_rows += 1
                    
            except KeyError:
                logger.warning(f"主键 {key} 在某个数据框中不存在")
                continue
            except Exception as e:
                logger.error(f"处理主键 {key} 时出错: {e}")
                continue
        
        return {
            'differences': differences,
            'identical_rows': identical_rows
        }
    
    def compare_csvs(self, file1: str, file2: str, 
                    output_html: str = "comparison_report.html") -> Dict[str, Any]:
        """
        对比两个CSV文件
        
        Args:
            file1, file2: CSV文件路径
            output_html: 输出HTML报告路径
            
        Returns:
            对比结果
        """
        logger.info("开始CSV对比流程")
        
        # 加载数据
        df1 = self.load_csv_chunked(file1)
        df2 = self.load_csv_chunked(file2)
        
        # 验证主键存在
        for key in self.primary_keys:
            if key not in df1.columns:
                raise ValueError(f"主键 '{key}' 不存在于文件1中")
            if key not in df2.columns:
                raise ValueError(f"主键 '{key}' 不存在于文件2中")
        
        # 智能抽样
        df1_sample, df2_sample = self.intelligent_sampling(df1, df2)
        
        # 执行对比
        results = self.parallel_compare_chunks(df1_sample, df2_sample)
        
        # 生成HTML报告
        self.generate_html_report(results, file1, file2, output_html)
        
        # 存储结果
        self.comparison_results = results
        
        return results
    
    def generate_html_report(self, results: Dict[str, Any], 
                           file1: str, file2: str, output_path: str):
        """
        生成HTML对比报告
        
        Args:
            results: 对比结果
            file1, file2: 源文件路径
            output_path: HTML输出路径
        """
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>CSV对比报告</title>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
        .summary {{ margin: 20px 0; }}
        .stats {{ display: flex; justify-content: space-around; margin: 20px 0; }}
        .stat-box {{ background-color: #e8f4f8; padding: 15px; border-radius: 5px; text-align: center; }}
        .differences {{ margin: 20px 0; }}
        .diff-row {{ border: 1px solid #ddd; margin: 10px 0; padding: 10px; border-radius: 5px; }}
        .diff-row:nth-child(even) {{ background-color: #f9f9f9; }}
        .field-diff {{ margin: 5px 0; padding: 5px; background-color: #fff3cd; border-radius: 3px; }}
        .key {{ font-weight: bold; color: #0066cc; }}
        .field-name {{ font-weight: bold; }}
        .value1 {{ color: #d32f2f; }}
        .value2 {{ color: #388e3c; }}
        .no-differences {{ color: #666; font-style: italic; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>CSV文件对比报告</h1>
        <p><strong>文件1:</strong> {file1}</p>
        <p><strong>文件2:</strong> {file2}</p>
        <p><strong>对比时间:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>主键:</strong> {', '.join(self.primary_keys)}</p>
    </div>

    <div class="summary">
        <h2>对比摘要</h2>
        <div class="stats">
            <div class="stat-box">
                <h3>{results['total_keys']}</h3>
                <p>总主键数</p>
            </div>
            <div class="stat-box">
                <h3>{results['common_keys']}</h3>
                <p>共同主键数</p>
            </div>
            <div class="stat-box">
                <h3>{results['only_in_df1']}</h3>
                <p>仅在文件1中</p>
            </div>
            <div class="stat-box">
                <h3>{results['only_in_df2']}</h3>
                <p>仅在文件2中</p>
            </div>
            <div class="stat-box">
                <h3>{results['identical_rows']}</h3>
                <p>完全相同行数</p>
            </div>
            <div class="stat-box">
                <h3>{len(results['differences'])}</h3>
                <p>存在差异行数</p>
            </div>
        </div>
    </div>

    <div class="differences">
        <h2>详细差异 (显示前100条)</h2>
        """
        
        if not results['differences']:
            html_content += '<p class="no-differences">未发现数据差异</p>'
        else:
            for i, diff in enumerate(results['differences'][:100]):
                html_content += f"""
                <div class="diff-row">
                    <p class="key">主键: {diff['key']}</p>
                """
                
                for field_diff in diff['differences']:
                    html_content += f"""
                    <div class="field-diff">
                        <span class="field-name">{field_diff['field']}:</span>
                        <span class="value1">文件1: {field_diff['value1']}</span> → 
                        <span class="value2">文件2: {field_diff['value2']}</span>
                    </div>
                    """
                
                html_content += "</div>"
        
        html_content += """
    </div>
    
    <div class="summary">
        <h2>建议的数据转换</h2>
        <p>基于对比结果，建议关注以下字段的转换规则:</p>
        <ul>
        """
        
        # 分析字段差异模式
        field_diff_count = {}
        for diff in results['differences']:
            for field_diff in diff['differences']:
                field_name = field_diff['field']
                field_diff_count[field_name] = field_diff_count.get(field_name, 0) + 1
        
        for field, count in sorted(field_diff_count.items(), key=lambda x: x[1], reverse=True):
            html_content += f"<li>{field}: {count} 处差异</li>"
        
        html_content += """
        </ul>
    </div>
</body>
</html>
        """
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML报告已生成: {output_path}")
    
    def export_differences_csv(self, output_path: str):
        """
        导出差异数据为CSV
        
        Args:
            output_path: 输出CSV路径
        """
        if not self.comparison_results or not self.comparison_results['differences']:
            logger.warning("无差异数据可导出")
            return
        
        diff_data = []
        for diff in self.comparison_results['differences']:
            for field_diff in diff['differences']:
                diff_data.append({
                    'primary_key': diff['key'],
                    'field': field_diff['field'],
                    'value_file1': field_diff['value1'],
                    'value_file2': field_diff['value2']
                })
        
        df_diff = pd.DataFrame(diff_data)
        df_diff.to_csv(output_path, index=False)
        logger.info(f"差异数据已导出: {output_path}")


def main():
    """命令行主函数"""
    parser = argparse.ArgumentParser(description='CSV自动对比工具')
    parser.add_argument('file1', help='第一个CSV文件路径')
    parser.add_argument('file2', help='第二个CSV文件路径')
    parser.add_argument('--keys', '-k', nargs='+', required=True, help='主键列名（支持多个）')
    parser.add_argument('--sample-rate', '-s', type=float, default=0.1, help='抽样率 (0-1)')
    parser.add_argument('--output', '-o', default='comparison_report.html', help='输出HTML报告路径')
    parser.add_argument('--export-csv', help='导出差异数据CSV路径')
    
    args = parser.parse_args()
    
    # 创建对比器
    comparator = CSVComparator(
        primary_keys=args.keys,
        sample_rate=args.sample_rate
    )
    
    try:
        # 执行对比
        results = comparator.compare_csvs(args.file1, args.file2, args.output)
        
        # 导出差异CSV（如果指定）
        if args.export_csv:
            comparator.export_differences_csv(args.export_csv)
        
        # 打印摘要
        print(f"\n=== 对比完成 ===")
        print(f"总主键数: {results['total_keys']}")
        print(f"共同主键数: {results['common_keys']}")
        print(f"相同行数: {results['identical_rows']}")
        print(f"差异行数: {len(results['differences'])}")
        print(f"HTML报告: {args.output}")
        
    except Exception as e:
        logger.error(f"对比过程出错: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())

