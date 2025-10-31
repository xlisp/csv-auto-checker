下面是改写后的代码，移除了主键依赖，使用整行对比的方式：

```python
#!/usr/bin/env python3
"""
CSV 自动对比工具
基于整行对比的高性能CSV对比，生成HTML报告显示差异
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
    """高性能CSV对比工具（无需主键）"""

    def __init__(self, sample_rate: float = 0.1, exclude_fields: List[str] = None):
        """
        初始化对比器

        Args:
            sample_rate: 智能抽样率（0-1之间）
            exclude_fields: 排除不比较的字段列表
        """
        self.sample_rate = sample_rate
        self.exclude_fields = exclude_fields or []
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

    def create_row_hash(self, df: pd.DataFrame) -> pd.Series:
        """
        为每行创建哈希值作为唯一标识

        Args:
            df: 数据框

        Returns:
            哈希值Series
        """
        # 排除指定字段后创建哈希
        compare_cols = [col for col in df.columns if col not in self.exclude_fields]
        
        def hash_row(row):
            row_str = '|'.join(str(row[col]) for col in compare_cols)
            return hashlib.md5(row_str.encode()).hexdigest()
        
        return df[compare_cols].apply(hash_row, axis=1)

    def intelligent_sampling(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        智能抽样策略

        Args:
            df1, df2: 待对比的数据框

        Returns:
            抽样后的数据框
        """
        # 创建行哈希
        hash1 = self.create_row_hash(df1)
        hash2 = self.create_row_hash(df2)

        df1_with_hash = df1.copy()
        df2_with_hash = df2.copy()
        df1_with_hash['__row_hash__'] = hash1
        df2_with_hash['__row_hash__'] = hash2

        logger.info(f"文件1哈希示例: {hash1.head(3).tolist()}")
        logger.info(f"文件2哈希示例: {hash2.head(3).tolist()}")

        # 找到共同的哈希（相同的行）
        common_hashes = set(hash1) & set(hash2)
        logger.info(f"找到 {len(common_hashes)} 个完全相同的行")

        # 找到不同的行
        only_in_df1 = set(hash1) - set(hash2)
        only_in_df2 = set(hash2) - set(hash1)
        
        logger.info(f"仅在文件1中的行: {len(only_in_df1)}")
        logger.info(f"仅在文件2中的行: {len(only_in_df2)}")

        # 如果抽样率为1.0，返回所有数据
        if self.sample_rate >= 1.0:
            return df1_with_hash, df2_with_hash

        # 对不同的行进行抽样
        sample_size_df1 = int(len(only_in_df1) * self.sample_rate)
        sample_size_df2 = int(len(only_in_df2) * self.sample_rate)
        
        sampled_hashes_df1 = random.sample(list(only_in_df1), min(sample_size_df1, len(only_in_df1)))
        sampled_hashes_df2 = random.sample(list(only_in_df2), min(sample_size_df2, len(only_in_df2)))

        # 筛选数据
        df1_sampled = df1_with_hash[df1_with_hash['__row_hash__'].isin(sampled_hashes_df1)]
        df2_sampled = df2_with_hash[df2_with_hash['__row_hash__'].isin(sampled_hashes_df2)]

        logger.info(f"智能抽样完成: {len(df1_sampled)} + {len(df2_sampled)} 行数据")
        return df1_sampled, df2_sampled

    def compare_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Dict[str, Any]:
        """
        对比两个数据框（不依赖主键）

        Args:
            df1, df2: 待对比的数据框

        Returns:
            对比结果字典
        """
        # 创建行哈希
        if '__row_hash__' not in df1.columns:
            df1 = df1.copy()
            df1['__row_hash__'] = self.create_row_hash(df1)
        
        if '__row_hash__' not in df2.columns:
            df2 = df2.copy()
            df2['__row_hash__'] = self.create_row_hash(df2)

        hash1_set = set(df1['__row_hash__'])
        hash2_set = set(df2['__row_hash__'])

        # 计算集合差异
        only_in_df1_hashes = hash1_set - hash2_set
        only_in_df2_hashes = hash2_set - hash1_set
        common_hashes = hash1_set & hash2_set

        # 提取仅在df1中的行
        df1_only = df1[df1['__row_hash__'].isin(only_in_df1_hashes)].drop('__row_hash__', axis=1)
        
        # 提取仅在df2中的行
        df2_only = df2[df2['__row_hash__'].isin(only_in_df2_hashes)].drop('__row_hash__', axis=1)

        results = {
            'total_rows_df1': len(df1),
            'total_rows_df2': len(df2),
            'identical_rows': len(common_hashes),
            'only_in_df1': len(only_in_df1_hashes),
            'only_in_df2': len(only_in_df2_hashes),
            'df1_only_data': df1_only,
            'df2_only_data': df2_only
        }

        logger.info(f"对比完成: {results['identical_rows']} 行相同")
        logger.info(f"仅在文件1中: {results['only_in_df1']} 行")
        logger.info(f"仅在文件2中: {results['only_in_df2']} 行")
        
        return results

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

        # 记录排除的字段
        if self.exclude_fields:
            logger.info(f"排除比较的字段: {', '.join(self.exclude_fields)}")

        # 智能抽样
        df1_sample, df2_sample = self.intelligent_sampling(df1, df2)

        # 执行对比
        results = self.compare_dataframes(df1_sample, df2_sample)

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
        .diff-table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        .diff-table th, .diff-table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        .diff-table th {{ background-color: #f2f2f2; font-weight: bold; }}
        .diff-table tr:nth-child(even) {{ background-color: #f9f9f9; }}
        .section-title {{ color: #0066cc; margin-top: 30px; }}
        .excluded-fields {{ background-color: #f8f9fa; padding: 10px; border-radius: 5px; margin: 10px 0; }}
        .no-data {{ color: #666; font-style: italic; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>CSV文件对比报告（无主键模式）</h1>
        <p><strong>文件1:</strong> {file1}</p>
        <p><strong>文件2:</strong> {file2}</p>
        <p><strong>对比时间:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>对比方式:</strong> 整行内容对比（无需主键）</p>"""

        if self.exclude_fields:
            html_content += f"""
        <div class="excluded-fields">
            <strong>排除比较的字段:</strong> {', '.join(self.exclude_fields)}
        </div>"""

        html_content += f"""
    </div>

    <div class="summary">
        <h2>对比摘要</h2>
        <div class="stats">
            <div class="stat-box">
                <h3>{results['total_rows_df1']}</h3>
                <p>文件1总行数</p>
            </div>
            <div class="stat-box">
                <h3>{results['total_rows_df2']}</h3>
                <p>文件2总行数</p>
            </div>
            <div class="stat-box">
                <h3>{results['identical_rows']}</h3>
                <p>完全相同行数</p>
            </div>
            <div class="stat-box">
                <h3>{results['only_in_df1']}</h3>
                <p>仅在文件1中</p>
            </div>
            <div class="stat-box">
                <h3>{results['only_in_df2']}</h3>
                <p>仅在文件2中</p>
            </div>
        </div>
    </div>

    <div class="differences">
        <h2 class="section-title">仅在文件1中的行 (前50条)</h2>
        """

        df1_only = results['df1_only_data']
        if len(df1_only) == 0:
            html_content += '<p class="no-data">无独有数据</p>'
        else:
            html_content += '<table class="diff-table">'
            html_content += '<tr>' + ''.join(f'<th>{col}</th>' for col in df1_only.columns) + '</tr>'
            
            for _, row in df1_only.head(50).iterrows():
                html_content += '<tr>' + ''.join(f'<td>{val}</td>' for val in row) + '</tr>'
            
            html_content += '</table>'

        html_content += """
        <h2 class="section-title">仅在文件2中的行 (前50条)</h2>
        """

        df2_only = results['df2_only_data']
        if len(df2_only) == 0:
            html_content += '<p class="no-data">无独有数据</p>'
        else:
            html_content += '<table class="diff-table">'
            html_content += '<tr>' + ''.join(f'<th>{col}</th>' for col in df2_only.columns) + '</tr>'
            
            for _, row in df2_only.head(50).iterrows():
                html_content += '<tr>' + ''.join(f'<td>{val}</td>' for val in row) + '</tr>'
            
            html_content += '</table>'

        html_content += """
    </div>

    <div class="summary">
        <h2>对比说明</h2>
        <ul>
            <li>本工具使用整行内容哈希进行对比，无需指定主键</li>
            <li>相同行：两个文件中内容完全一致的行</li>
            <li>仅在文件1中：在文件1中存在但文件2中不存在的行</li>
            <li>仅在文件2中：在文件2中存在但文件1中不存在的行</li>
        </ul>
    </div>
</body>
</html>
        """

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        logger.info(f"HTML报告已生成: {output_path}")

    def export_differences_csv(self, output_path_prefix: str):
        """
        导出差异数据为CSV

        Args:
            output_path_prefix: 输出CSV路径前缀
        """
        if not self.comparison_results:
            logger.warning("无差异数据可导出")
            return

        # 导出仅在文件1中的数据
        df1_only = self.comparison_results['df1_only_data']
        if len(df1_only) > 0:
            output1 = f"{output_path_prefix}_only_in_file1.csv"
            df1_only.to_csv(output1, index=False)
            logger.info(f"仅在文件1中的数据已导出: {output1}")

        # 导出仅在文件2中的数据
        df2_only = self.comparison_results['df2_only_data']
        if len(df2_only) > 0:
            output2 = f"{output_path_prefix}_only_in_file2.csv"
            df2_only.to_csv(output2, index=False)
            logger.info(f"仅在文件2中的数据已导出: {output2}")


def main():
    """命令行主函数"""
    parser = argparse.ArgumentParser(description='CSV自动对比工具（无需主键）')
    parser.add_argument('file1', help='第一个CSV文件路径')
    parser.add_argument('file2', help='第二个CSV文件路径')
    parser.add_argument('--exclude', '-e', nargs='+', default=[], dest='exclude_fields',
                       help='排除不比较的字段列表')
    parser.add_argument('--sample-rate', '-s', type=float, default=1.0, help='抽样率 (0-1)，默认1.0全量对比')
    parser.add_argument('--output', '-o', default='comparison_report.html', help='输出HTML报告路径')
    parser.add_argument('--export-csv', help='导出差异数据CSV路径前缀')

    args = parser.parse_args()

    # 创建对比器
    comparator = CSVComparator(
        sample_rate=args.sample_rate,
        exclude_fields=args.exclude_fields
    )

    try:
        # 执行对比
        results = comparator.compare_csvs(args.file1, args.file2, args.output)

        # 导出差异CSV（如果指定）
        if args.export_csv:
            comparator.export_differences_csv(args.export_csv)

        # 打印摘要
        print(f"\n=== 对比完成 ===")
        print(f"文件1总行数: {results['total_rows_df1']}")
        print(f"文件2总行数: {results['total_rows_df2']}")
        print(f"相同行数: {results['identical_rows']}")
        print(f"仅在文件1中: {results['only_in_df1']} 行")
        print(f"仅在文件2中: {results['only_in_df2']} 行")
        print(f"HTML报告: {args.output}")

    except Exception as e:
        logger.error(f"对比过程出错: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
```

**主要改动：**

1. **移除主键参数**：不再需要 `--keys` 参数
2. **整行哈希对比**：使用 `create_row_hash()` 为每行创建MD5哈希值作为唯一标识
3. **简化对比逻辑**：使用集合运算找出差异行，类似 Spark 的 `except()`
4. **改进抽样策略**：只对不同的行进行抽样，相同的行直接统计
5. **修改报告格式**：显示完整的差异行数据，而非字段级差异
6. **调整导出功能**：分别导出"仅在文件1"和"仅在文件2"的数据

**使用示例：**

```bash
# 全量对比
python csv_comparator.py file1.csv file2.csv

# 排除某些字段
python csv_comparator.py file1.csv file2.csv --exclude timestamp update_time

# 10%抽样对比
python csv_comparator.py file1.csv file2.csv --sample-rate 0.1

# 导出差异数据
python csv_comparator.py file1.csv file2.csv --export-csv diff_output
```

