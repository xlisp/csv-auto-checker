下面是使用 PySpark 的 `exceptAll()` 方法重写的版本：

```python
#!/usr/bin/env python3
"""
CSV 自动对比工具 - PySpark版本
使用 Spark DataFrame 的 exceptAll 方法进行高性能对比
"""

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, concat_ws, md5, monotonically_increasing_id
from typing import List, Dict, Any, Tuple
from datetime import datetime
import logging
import argparse

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SparkCSVComparator:
    """基于PySpark的CSV对比工具"""

    def __init__(self, exclude_fields: List[str] = None, sample_rate: float = 1.0):
        """
        初始化对比器

        Args:
            exclude_fields: 排除不比较的字段列表
            sample_rate: 抽样率（0-1之间）
        """
        self.exclude_fields = exclude_fields or []
        self.sample_rate = sample_rate
        self.comparison_results = {}
        
        # 初始化Spark Session
        self.spark = SparkSession.builder \
            .appName("CSV_Comparator") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        logger.info("Spark Session 已创建")

    def load_csv(self, file_path: str, header: bool = True, 
                 infer_schema: bool = True) -> DataFrame:
        """
        加载CSV文件为Spark DataFrame

        Args:
            file_path: CSV文件路径
            header: 是否包含表头
            infer_schema: 是否自动推断schema

        Returns:
            Spark DataFrame
        """
        logger.info(f"开始加载CSV文件: {file_path}")
        
        try:
            df = self.spark.read.csv(
                file_path,
                header=header,
                inferSchema=infer_schema,
                encoding='utf-8'
            )
            
            count = df.count()
            logger.info(f"成功加载 {count} 行数据，{len(df.columns)} 列")
            logger.info(f"列名: {df.columns}")
            
            return df
            
        except Exception as e:
            logger.error(f"加载CSV文件失败: {e}")
            raise

    def prepare_dataframe(self, df: DataFrame) -> DataFrame:
        """
        准备DataFrame用于对比（移除排除字段）

        Args:
            df: 原始DataFrame

        Returns:
            处理后的DataFrame
        """
        if self.exclude_fields:
            logger.info(f"排除字段: {', '.join(self.exclude_fields)}")
            compare_cols = [c for c in df.columns if c not in self.exclude_fields]
            df = df.select(compare_cols)
        
        # 如果需要抽样
        if self.sample_rate < 1.0:
            logger.info(f"执行抽样，抽样率: {self.sample_rate}")
            df = df.sample(fraction=self.sample_rate, seed=42)
        
        return df

    def compare_dataframes(self, df1: DataFrame, df2: DataFrame) -> Dict[str, Any]:
        """
        使用 exceptAll 对比两个DataFrame

        Args:
            df1: 第一个DataFrame
            df2: 第二个DataFrame

        Returns:
            对比结果字典
        """
        logger.info("开始对比DataFrame...")
        
        # 确保两个DataFrame的列顺序一致
        if set(df1.columns) != set(df2.columns):
            raise ValueError("两个CSV文件的列不一致！")
        
        # 统一列顺序
        columns = sorted(df1.columns)
        df1 = df1.select(columns)
        df2 = df2.select(columns)
        
        # 统计总行数
        total_df1 = df1.count()
        total_df2 = df2.count()
        
        logger.info(f"DataFrame1 行数: {total_df1}")
        logger.info(f"DataFrame2 行数: {total_df2}")
        
        # 使用 exceptAll 找出差异
        # df1.exceptAll(df2): 在df1中但不在df2中的行（保留重复）
        only_in_df1 = df1.exceptAll(df2)
        
        # df2.exceptAll(df1): 在df2中但不在df1中的行（保留重复）
        only_in_df2 = df2.exceptAll(df1)
        
        # 缓存结果以提高性能
        only_in_df1.cache()
        only_in_df2.cache()
        
        count_only_df1 = only_in_df1.count()
        count_only_df2 = only_in_df2.count()
        
        logger.info(f"仅在文件1中: {count_only_df1} 行")
        logger.info(f"仅在文件2中: {count_only_df2} 行")
        
        # 计算相同的行数
        identical_rows = total_df1 - count_only_df1
        
        results = {
            'total_rows_df1': total_df1,
            'total_rows_df2': total_df2,
            'identical_rows': identical_rows,
            'only_in_df1': count_only_df1,
            'only_in_df2': count_only_df2,
            'df1_only_data': only_in_df1,
            'df2_only_data': only_in_df2,
            'columns': columns
        }
        
        return results

    def compare_csvs(self, file1: str, file2: str,
                    output_html: str = "comparison_report.html",
                    export_csv_prefix: str = None) -> Dict[str, Any]:
        """
        对比两个CSV文件

        Args:
            file1, file2: CSV文件路径
            output_html: 输出HTML报告路径
            export_csv_prefix: 导出CSV的路径前缀

        Returns:
            对比结果
        """
        logger.info("=" * 60)
        logger.info("开始CSV对比流程")
        logger.info("=" * 60)
        
        # 加载数据
        df1 = self.load_csv(file1)
        df2 = self.load_csv(file2)
        
        # 准备数据（移除排除字段、抽样）
        df1_prepared = self.prepare_dataframe(df1)
        df2_prepared = self.prepare_dataframe(df2)
        
        # 执行对比
        results = self.compare_dataframes(df1_prepared, df2_prepared)
        
        # 导出差异数据到CSV
        if export_csv_prefix:
            self.export_differences_csv(results, export_csv_prefix)
        
        # 生成HTML报告
        self.generate_html_report(results, file1, file2, output_html)
        
        # 存储结果
        self.comparison_results = results
        
        return results

    def export_differences_csv(self, results: Dict[str, Any], output_prefix: str):
        """
        导出差异数据为CSV

        Args:
            results: 对比结果
            output_prefix: 输出路径前缀
        """
        logger.info("开始导出差异数据...")
        
        # 导出仅在文件1中的数据
        if results['only_in_df1'] > 0:
            output1 = f"{output_prefix}_only_in_file1.csv"
            results['df1_only_data'].coalesce(1).write.csv(
                output1,
                header=True,
                mode='overwrite'
            )
            logger.info(f"仅在文件1中的数据已导出: {output1}")
        
        # 导出仅在文件2中的数据
        if results['only_in_df2'] > 0:
            output2 = f"{output_prefix}_only_in_file2.csv"
            results['df2_only_data'].coalesce(1).write.csv(
                output2,
                header=True,
                mode='overwrite'
            )
            logger.info(f"仅在文件2中的数据已导出: {output2}")

    def generate_html_report(self, results: Dict[str, Any],
                           file1: str, file2: str, output_path: str):
        """
        生成HTML对比报告

        Args:
            results: 对比结果
            file1, file2: 源文件路径
            output_path: HTML输出路径
        """
        logger.info("开始生成HTML报告...")
        
        # 收集样本数据用于展示
        df1_sample = []
        df2_sample = []
        
        if results['only_in_df1'] > 0:
            df1_sample = results['df1_only_data'].limit(50).collect()
        
        if results['only_in_df2'] > 0:
            df2_sample = results['df2_only_data'].limit(50).collect()
        
        columns = results['columns']
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>CSV对比报告 - PySpark版本</title>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
        .container {{ max-width: 1400px; margin: 0 auto; background-color: white; padding: 30px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 30px; }}
        .header h1 {{ margin: 0 0 15px 0; }}
        .header p {{ margin: 5px 0; opacity: 0.9; }}
        .summary {{ margin: 30px 0; }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
        .stat-box {{ 
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            color: white;
            padding: 25px;
            border-radius: 10px;
            text-align: center;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .stat-box h3 {{ margin: 0; font-size: 2.5em; font-weight: bold; }}
        .stat-box p {{ margin: 10px 0 0 0; font-size: 1.1em; opacity: 0.9; }}
        .differences {{ margin: 30px 0; }}
        .section-title {{ 
            color: #667eea;
            font-size: 1.5em;
            margin: 30px 0 15px 0;
            padding-bottom: 10px;
            border-bottom: 3px solid #667eea;
        }}
        .diff-table {{ 
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .diff-table th {{ 
            background-color: #667eea;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: bold;
        }}
        .diff-table td {{ 
            border: 1px solid #ddd;
            padding: 10px;
            text-align: left;
        }}
        .diff-table tr:nth-child(even) {{ background-color: #f9f9f9; }}
        .diff-table tr:hover {{ background-color: #f0f0f0; }}
        .excluded-fields {{ 
            background-color: #fff3cd;
            border-left: 4px solid #ffc107;
            padding: 15px;
            border-radius: 5px;
            margin: 15px 0;
        }}
        .no-data {{ 
            color: #666;
            font-style: italic;
            text-align: center;
            padding: 30px;
            background-color: #f9f9f9;
            border-radius: 5px;
        }}
        .badge {{ 
            display: inline-block;
            padding: 5px 10px;
            background-color: #667eea;
            color: white;
            border-radius: 5px;
            font-size: 0.9em;
            margin-left: 10px;
        }}
        .info-box {{
            background-color: #e3f2fd;
            border-left: 4px solid #2196f3;
            padding: 15px;
            border-radius: 5px;
            margin: 20px 0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 CSV文件对比报告</h1>
            <p><strong>文件1:</strong> {file1}</p>
            <p><strong>文件2:</strong> {file2}</p>
            <p><strong>对比时间:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><strong>对比引擎:</strong> Apache Spark (exceptAll方法) <span class="badge">高性能</span></p>
        </div>
"""

        if self.exclude_fields:
            html_content += f"""
        <div class="excluded-fields">
            <strong>⚠️ 排除比较的字段:</strong> {', '.join(self.exclude_fields)}
        </div>
"""

        if self.sample_rate < 1.0:
            html_content += f"""
        <div class="info-box">
            <strong>📊 抽样信息:</strong> 使用 {self.sample_rate * 100}% 抽样率进行对比
        </div>
"""

        html_content += f"""
        <div class="summary">
            <h2 class="section-title">📈 对比摘要</h2>
            <div class="stats">
                <div class="stat-box">
                    <h3>{results['total_rows_df1']:,}</h3>
                    <p>文件1总行数</p>
                </div>
                <div class="stat-box">
                    <h3>{results['total_rows_df2']:,}</h3>
                    <p>文件2总行数</p>
                </div>
                <div class="stat-box">
                    <h3>{results['identical_rows']:,}</h3>
                    <p>完全相同行数</p>
                </div>
                <div class="stat-box">
                    <h3>{results['only_in_df1']:,}</h3>
                    <p>仅在文件1中</p>
                </div>
                <div class="stat-box">
                    <h3>{results['only_in_df2']:,}</h3>
                    <p>仅在文件2中</p>
                </div>
            </div>
        </div>

        <div class="differences">
            <h2 class="section-title">📋 仅在文件1中的行 (前50条)</h2>
"""

        if not df1_sample:
            html_content += '<p class="no-data">✓ 无独有数据</p>'
        else:
            html_content += '<div style="overflow-x: auto;"><table class="diff-table">'
            html_content += '<tr>' + ''.join(f'<th>{col}</th>' for col in columns) + '</tr>'
            
            for row in df1_sample:
                html_content += '<tr>' + ''.join(f'<td>{row[col]}</td>' for col in columns) + '</tr>'
            
            html_content += '</table></div>'

        html_content += """
            <h2 class="section-title">📋 仅在文件2中的行 (前50条)</h2>
"""

        if not df2_sample:
            html_content += '<p class="no-data">✓ 无独有数据</p>'
        else:
            html_content += '<div style="overflow-x: auto;"><table class="diff-table">'
            html_content += '<tr>' + ''.join(f'<th>{col}</th>' for col in columns) + '</tr>'
            
            for row in df2_sample:
                html_content += '<tr>' + ''.join(f'<td>{row[col]}</td>' for col in columns) + '</tr>'
            
            html_content += '</table></div>'

        html_content += """
        </div>

        <div class="summary">
            <h2 class="section-title">ℹ️ 对比说明</h2>
            <div class="info-box">
                <ul style="margin: 10px 0; padding-left: 20px;">
                    <li><strong>对比方法:</strong> 使用 Apache Spark 的 <code>exceptAll()</code> 方法进行分布式对比</li>
                    <li><strong>相同行:</strong> 两个文件中内容完全一致的行（包括重复行）</li>
                    <li><strong>仅在文件1中:</strong> 在文件1中存在但文件2中不存在的行</li>
                    <li><strong>仅在文件2中:</strong> 在文件2中存在但文件1中不存在的行</li>
                    <li><strong>性能优势:</strong> 使用Spark分布式计算，支持大规模数据集对比</li>
                </ul>
            </div>
        </div>
    </div>
</body>
</html>
"""

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        logger.info(f"HTML报告已生成: {output_path}")

    def stop(self):
        """停止Spark Session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark Session 已停止")


def main():
    """命令行主函数"""
    parser = argparse.ArgumentParser(
        description='CSV自动对比工具 - PySpark版本 (使用exceptAll方法)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 基本对比
  python spark_csv_comparator.py file1.csv file2.csv
  
  # 排除某些字段
  python spark_csv_comparator.py file1.csv file2.csv --exclude timestamp update_time
  
  # 10%抽样对比
  python spark_csv_comparator.py file1.csv file2.csv --sample-rate 0.1
  
  # 导出差异数据
  python spark_csv_comparator.py file1.csv file2.csv --export-csv diff_output
        """
    )
    
    parser.add_argument('file1', help='第一个CSV文件路径')
    parser.add_argument('file2', help='第二个CSV文件路径')
    parser.add_argument('--exclude', '-e', nargs='+', default=[], 
                       dest='exclude_fields',
                       help='排除不比较的字段列表')
    parser.add_argument('--sample-rate', '-s', type=float, default=1.0,
                       help='抽样率 (0-1)，默认1.0全量对比')
    parser.add_argument('--output', '-o', default='comparison_report.html',
                       help='输出HTML报告路径')
    parser.add_argument('--export-csv', help='导出差异数据CSV路径前缀')

    args = parser.parse_args()

    # 验证抽样率
    if not 0 < args.sample_rate <= 1.0:
        parser.error("抽样率必须在 0 和 1 之间")

    # 创建对比器
    comparator = SparkCSVComparator(
        exclude_fields=args.exclude_fields,
        sample_rate=args.sample_rate
    )

    try:
        # 执行对比
        results = comparator.compare_csvs(
            args.file1,
            args.file2,
            args.output,
            args.export_csv
        )

        # 打印摘要
        print("\n" + "=" * 60)
        print("📊 对比完成")
        print("=" * 60)
        print(f"文件1总行数:    {results['total_rows_df1']:,}")
        print(f"文件2总行数:    {results['total_rows_df2']:,}")
        print(f"相同行数:      {results['identical_rows']:,}")
        print(f"仅在文件1中:   {results['only_in_df1']:,} 行")
        print(f"仅在文件2中:   {results['only_in_df2']:,} 行")
        print(f"HTML报告:      {args.output}")
        
        if args.export_csv:
            print(f"差异数据已导出到: {args.export_csv}_only_in_file*.csv")
        
        print("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"对比过程出错: {e}", exc_info=True)
        return 1

    finally:
        # 停止Spark Session
        comparator.stop()


if __name__ == "__main__":
    exit(main())
```

**主要特点：**

1. **使用 PySpark 的 `exceptAll()` 方法**：
   - `df1.exceptAll(df2)` - 找出在 df1 中但不在 df2 中的行
   - `df2.exceptAll(df1)` - 找出在 df2 中但不在 df1 中的行
   - 保留重复行，完全等同于 Spark 的行为

2. **分布式计算**：
   - 支持大规模数据集
   - 自动并行处理
   - 配置了自适应查询执行

3. **无需主键**：
   - 整行内容对比
   - 不需要指定任何主键列

4. **功能完整**：
   - 支持排除字段
   - 支持抽样对比
   - 生成HTML报告
   - 导出差异数据到CSV

**使用方法：**

```bash
# 安装PySpark
pip install pyspark

# 基本对比
python spark_csv_comparator.py file1.csv file2.csv

# 排除某些字段
python spark_csv_comparator.py file1.csv file2.csv --exclude id timestamp

# 10%抽样
python spark_csv_comparator.py file1.csv file2.csv --sample-rate 0.1

# 导出差异
python spark_csv_comparator.py file1.csv file2.csv --export-csv diff_result
```

这个版本使用了真正的 Spark `exceptAll()` 方法，性能更好，特别适合大数据场景！

