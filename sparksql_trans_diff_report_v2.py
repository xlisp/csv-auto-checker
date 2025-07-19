#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark SQL验证器
接受Spark SQL语句和两个文件作为参数，比对转换结果并生成HTML报表
"""

import argparse
import os
import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hash, md5, concat_ws
import pandas as pd
from typing import Dict, List, Tuple, Any

class SparkSQLValidator:
    def __init__(self, app_name="SparkSQL_Validator"):
        try:
            # 初始化Spark会话
            self.spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.shuffle.partitions", "200") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            self.comparison_results = {}
            self.execution_log = []
            self.log_message("Spark会话初始化成功")
        except Exception as e:
            print(f"Spark会话初始化失败: {str(e)}")
            sys.exit(1)
    
    def log_message(self, message: str, level: str = "INFO"):
        """记录日志消息"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}"
        print(log_entry)
        self.execution_log.append(log_entry)
    
    def read_sql_from_file(self, sql_path: str) -> str:
        """从文件读取SQL语句"""
        try:
            if not os.path.exists(sql_path):
                raise FileNotFoundError(f"SQL文件不存在: {sql_path}")
            
            with open(sql_path, 'r', encoding='utf-8') as f:
                sql_content = f.read().strip()
            
            self.log_message(f"成功读取SQL文件: {sql_path}")
            return sql_content
            
        except Exception as e:
            self.log_message(f"读取SQL文件失败 {sql_path}: {str(e)}", "ERROR")
            raise
    
    def read_csv_file(self, file_path: str, table_name: str) -> bool:
        """读取CSV文件并注册为临时表"""
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"文件不存在: {file_path}")
            
            self.log_message(f"开始读取CSV文件: {file_path}")
            
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("encoding", "UTF-8") \
                .option("multiline", "true") \
                .option("escape", '"') \
                .csv(file_path)
            
            # 注册临时表
            df.createOrReplaceTempView(table_name)
            
            row_count = df.count()
            col_count = len(df.columns)
            
            self.log_message(f"成功读取文件 {file_path} -> 表 {table_name}")
            self.log_message(f"数据维度: {row_count} 行 x {col_count} 列")
            self.log_message(f"字段列表: {', '.join(df.columns)}")
            
            return True
            
        except Exception as e:
            self.log_message(f"读取文件失败 {file_path}: {str(e)}", "ERROR")
            return False
    
    def execute_spark_sql(self, sql_query: str, result_table: str = "transform_result"):
        """执行Spark SQL并返回结果DataFrame"""
        try:
            self.log_message("开始执行Spark SQL转换...")
            self.log_message(f"SQL语句:\n{sql_query}")
            
            # 执行SQL
            result_df = self.spark.sql(sql_query)
            
            # 注册结果表
            result_df.createOrReplaceTempView(result_table)
            
            row_count = result_df.count()
            col_count = len(result_df.columns)
            
            self.log_message(f"SQL执行成功，结果维度: {row_count} 行 x {col_count} 列")
            self.log_message(f"结果字段: {', '.join(result_df.columns)}")
            
            return result_df
            
        except Exception as e:
            self.log_message(f"SQL执行失败: {str(e)}", "ERROR")
            raise
    
    def load_expected_results(self, expected_file: str, expected_table: str = "expected_result"):
        """加载期望结果文件"""
        try:
            if not self.read_csv_file(expected_file, expected_table):
                raise Exception("无法读取期望结果文件")
            
            expected_df = self.spark.table(expected_table)
            self.log_message("期望结果文件加载成功")
            
            return expected_df
            
        except Exception as e:
            self.log_message(f"加载期望结果失败: {str(e)}", "ERROR")
            raise
    
    def compare_schemas(self, actual_df, expected_df) -> Dict[str, Any]:
        """比较数据结构"""
        actual_schema = {field.name: str(field.dataType) for field in actual_df.schema.fields}
        expected_schema = {field.name: str(field.dataType) for field in expected_df.schema.fields}
        
        actual_columns = set(actual_schema.keys())
        expected_columns = set(expected_schema.keys())
        
        schema_comparison = {
            "actual_columns": sorted(actual_columns),
            "expected_columns": sorted(expected_columns),
            "missing_columns": sorted(expected_columns - actual_columns),
            "extra_columns": sorted(actual_columns - expected_columns),
            "common_columns": sorted(actual_columns & expected_columns),
            "data_type_matches": {},
            "data_type_mismatches": {}
        }
        
        # 检查数据类型
        for col_name in schema_comparison["common_columns"]:
            actual_type = actual_schema[col_name]
            expected_type = expected_schema[col_name]
            
            if actual_type == expected_type:
                schema_comparison["data_type_matches"][col_name] = actual_type
            else:
                schema_comparison["data_type_mismatches"][col_name] = {
                    "actual": actual_type,
                    "expected": expected_type
                }
        
        return schema_comparison
    
    def compare_data_content(self, actual_df, expected_df, common_columns: List[str]) -> Dict[str, Any]:
        """比较数据内容"""
        try:
            # 只比较共同列
            actual_subset = actual_df.select(*common_columns)
            expected_subset = expected_df.select(*common_columns)
            
            actual_count = actual_subset.count()
            expected_count = expected_subset.count()
            
            # 计算行级别的哈希值进行比较
            actual_with_hash = actual_subset.withColumn(
                "row_hash", 
                md5(concat_ws("|", *[col(c).cast("string") for c in common_columns]))
            )
            
            expected_with_hash = expected_subset.withColumn(
                "row_hash",
                md5(concat_ws("|", *[col(c).cast("string") for c in common_columns]))
            )
            
            # 注册临时表进行比较
            actual_with_hash.createOrReplaceTempView("actual_hashed")
            expected_with_hash.createOrReplaceTempView("expected_hashed")
            
            # 找出差异
            only_in_actual = self.spark.sql("""
                SELECT * FROM actual_hashed 
                WHERE row_hash NOT IN (SELECT row_hash FROM expected_hashed)
            """)
            
            only_in_expected = self.spark.sql("""
                SELECT * FROM expected_hashed 
                WHERE row_hash NOT IN (SELECT row_hash FROM actual_hashed)
            """)
            
            # 统计匹配的行
            matching_rows = self.spark.sql("""
                SELECT COUNT(*) as count FROM actual_hashed a
                INNER JOIN expected_hashed e ON a.row_hash = e.row_hash
            """).collect()[0]['count']
            
            content_comparison = {
                "actual_row_count": actual_count,
                "expected_row_count": expected_count,
                "matching_rows": matching_rows,
                "rows_only_in_actual": only_in_actual.count(),
                "rows_only_in_expected": only_in_expected.count(),
                "data_match_percentage": (matching_rows / max(actual_count, expected_count) * 100) if max(actual_count, expected_count) > 0 else 0,
                "sample_differences": {
                    "only_in_actual": only_in_actual.limit(10).toPandas().to_dict('records') if only_in_actual.count() > 0 else [],
                    "only_in_expected": only_in_expected.limit(10).toPandas().to_dict('records') if only_in_expected.count() > 0 else []
                }
            }
            
            return content_comparison
            
        except Exception as e:
            self.log_message(f"数据内容比较失败: {str(e)}", "ERROR")
            return {"error": str(e)}
    
    def generate_column_statistics(self, df, table_name: str) -> Dict[str, Any]:
        """生成列统计信息"""
        try:
            stats = {}
            df.createOrReplaceTempView(f"stats_{table_name}")
            
            for column in df.columns:
                col_type = str(df.schema[column].dataType)
                
                if "string" in col_type.lower():
                    # 字符串列统计
                    col_stats = self.spark.sql(f"""
                        SELECT 
                            COUNT(*) as total_count,
                            COUNT(`{column}`) as non_null_count,
                            COUNT(DISTINCT `{column}`) as distinct_count,
                            MIN(LENGTH(`{column}`)) as min_length,
                            MAX(LENGTH(`{column}`)) as max_length
                        FROM stats_{table_name}
                    """).collect()[0].asDict()
                
                elif any(x in col_type.lower() for x in ['int', 'long', 'float', 'double', 'decimal']):
                    # 数值列统计
                    col_stats = self.spark.sql(f"""
                        SELECT 
                            COUNT(*) as total_count,
                            COUNT(`{column}`) as non_null_count,
                            COUNT(DISTINCT `{column}`) as distinct_count,
                            MIN(`{column}`) as min_value,
                            MAX(`{column}`) as max_value,
                            AVG(`{column}`) as avg_value
                        FROM stats_{table_name}
                    """).collect()[0].asDict()
                
                else:
                    # 其他类型基本统计
                    col_stats = self.spark.sql(f"""
                        SELECT 
                            COUNT(*) as total_count,
                            COUNT(`{column}`) as non_null_count,
                            COUNT(DISTINCT `{column}`) as distinct_count
                        FROM stats_{table_name}
                    """).collect()[0].asDict()
                
                stats[column] = {
                    "data_type": col_type,
                    **col_stats
                }
            
            return stats
            
        except Exception as e:
            self.log_message(f"统计信息生成失败: {str(e)}", "ERROR")
            return {}
    
    def validate_transformation(self, source_file: str, expected_file: str, spark_sql: str) -> Dict[str, Any]:
        """验证数据转换"""
        self.log_message("开始数据转换验证流程...")
        
        validation_results = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source_file": source_file,
            "expected_file": expected_file,
            "spark_sql": spark_sql,
            "success": False,
            "schema_comparison": {},
            "content_comparison": {},
            "statistics": {},
            "summary": {}
        }
        
        try:
            # 1. 读取源数据文件
            if not self.read_csv_file(source_file, "source_table"):
                validation_results["error"] = "无法读取源数据文件"
                return validation_results
            
            # 2. 执行Spark SQL转换
            actual_df = self.execute_spark_sql(spark_sql)
            if actual_df is None:
                validation_results["error"] = "Spark SQL执行失败"
                return validation_results
            
            # 3. 加载期望结果
            expected_df = self.load_expected_results(expected_file)
            if expected_df is None:
                validation_results["error"] = "无法加载期望结果文件"
                return validation_results
            
            # 4. 比较数据结构
            schema_comparison = self.compare_schemas(actual_df, expected_df)
            validation_results["schema_comparison"] = schema_comparison
            
            # 5. 比较数据内容（仅比较共同列）
            if schema_comparison["common_columns"]:
                content_comparison = self.compare_data_content(
                    actual_df, expected_df, schema_comparison["common_columns"]
                )
                validation_results["content_comparison"] = content_comparison
            
            # 6. 生成统计信息
            validation_results["statistics"] = {
                "actual_data": self.generate_column_statistics(actual_df, "actual"),
                "expected_data": self.generate_column_statistics(expected_df, "expected")
            }
            
            # 7. 生成验证摘要
            is_schema_match = (
                len(schema_comparison["missing_columns"]) == 0 and
                len(schema_comparison["extra_columns"]) == 0 and
                len(schema_comparison["data_type_mismatches"]) == 0
            )
            
            is_content_match = False
            if "content_comparison" in validation_results and "error" not in validation_results["content_comparison"]:
                content_comp = validation_results["content_comparison"]
                is_content_match = (
                    content_comp.get("data_match_percentage", 0) == 100.0 and
                    content_comp.get("rows_only_in_actual", 0) == 0 and
                    content_comp.get("rows_only_in_expected", 0) == 0
                )
            
            validation_results["summary"] = {
                "overall_success": is_schema_match and is_content_match,
                "schema_match": is_schema_match,
                "content_match": is_content_match,
                "match_percentage": validation_results.get("content_comparison", {}).get("data_match_percentage", 0)
            }
            
            validation_results["success"] = True
            self.log_message("验证流程完成")
            
        except Exception as e:
            self.log_message(f"验证过程出错: {str(e)}", "ERROR")
            validation_results["error"] = str(e)
        
        return validation_results
    
    def generate_html_report(self, validation_results: Dict[str, Any], output_path: str = "validation_report.html"):
        """生成HTML验证报告"""
        try:
            html_template = """
            <!DOCTYPE html>
            <html lang="zh-CN">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Spark SQL 验证报告</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
                    .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                    .header {{ text-align: center; margin-bottom: 30px; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border-radius: 8px; }}
                    .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }}
                    .summary-card {{ padding: 20px; border-radius: 8px; text-align: center; color: white; }}
                    .success {{ background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); }}
                    .warning {{ background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); }}
                    .info {{ background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); }}
                    .section {{ margin-bottom: 30px; }}
                    .section-title {{ font-size: 1.5em; font-weight: bold; margin-bottom: 15px; padding: 10px; background: #f8f9fa; border-left: 4px solid #007bff; }}
                    table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
                    th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
                    th {{ background-color: #f8f9fa; font-weight: bold; }}
                    .highlight {{ background-color: #fff3cd; }}
                    .error {{ background-color: #f8d7da; }}
                    .success-row {{ background-color: #d4edda; }}
                    .code-block {{ background: #f8f9fa; padding: 15px; border-radius: 4px; font-family: monospace; white-space: pre-wrap; overflow-x: auto; border-left: 4px solid #007bff; }}
                    .log-entry {{ padding: 5px; border-bottom: 1px solid #eee; font-family: monospace; font-size: 0.9em; }}
                    .log-error {{ color: #dc3545; }}
                    .log-info {{ color: #17a2b8; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>🔍 Spark SQL 验证报告</h1>
                        <p>生成时间: {timestamp}</p>
                    </div>
                    
                    <div class="summary">
                        <div class="summary-card {overall_status}">
                            <h3>总体状态</h3>
                            <h2>{overall_result}</h2>
                        </div>
                        <div class="summary-card {schema_status}">
                            <h3>结构匹配</h3>
                            <h2>{schema_result}</h2>
                        </div>
                        <div class="summary-card {content_status}">
                            <h3>内容匹配度</h3>
                            <h2>{content_percentage:.1f}%</h2>
                        </div>
                    </div>
                    
                    <div class="section">
                        <div class="section-title">📋 基本信息</div>
                        <table>
                            <tr><th>源数据文件</th><td>{source_file}</td></tr>
                            <tr><th>期望结果文件</th><td>{expected_file}</td></tr>
                            <tr><th>执行时间</th><td>{timestamp}</td></tr>
                        </table>
                    </div>
                    
                    <div class="section">
                        <div class="section-title">💾 Spark SQL 语句</div>
                        <div class="code-block">{spark_sql}</div>
                    </div>
                    
                    {schema_section}
                    
                    {content_section}
                    
                    {statistics_section}
                    
                    {log_section}
                </div>
            </body>
            </html>
            """
            
            # 准备模板变量
            summary = validation_results.get("summary", {})
            
            overall_success = summary.get("overall_success", False)
            schema_match = summary.get("schema_match", False)
            content_percentage = summary.get("match_percentage", 0)
            
            template_vars = {
                "timestamp": validation_results.get("timestamp", ""),
                "source_file": validation_results.get("source_file", ""),
                "expected_file": validation_results.get("expected_file", ""),
                "spark_sql": validation_results.get("spark_sql", ""),
                "overall_status": "success" if overall_success else "warning",
                "overall_result": "✅ 通过" if overall_success else "❌ 失败",
                "schema_status": "success" if schema_match else "warning",
                "schema_result": "✅ 匹配" if schema_match else "❌ 不匹配",
                "content_status": "success" if content_percentage == 100 else "warning" if content_percentage > 80 else "info",
                "content_percentage": content_percentage
            }
            
            # 生成各个部分的HTML
            template_vars["schema_section"] = self._generate_schema_section(validation_results.get("schema_comparison", {}))
            template_vars["content_section"] = self._generate_content_section(validation_results.get("content_comparison", {}))
            template_vars["statistics_section"] = self._generate_statistics_section(validation_results.get("statistics", {}))
            template_vars["log_section"] = self._generate_log_section()
            
            # 生成最终HTML
            html_content = html_template.format(**template_vars)
            
            # 写入文件
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            self.log_message(f"HTML报告已生成: {output_path}")
            
        except Exception as e:
            self.log_message(f"生成HTML报告失败: {str(e)}", "ERROR")
    
    def _generate_schema_section(self, schema_comparison: Dict) -> str:
        """生成结构比较部分的HTML"""
        if not schema_comparison:
            return ""
        
        html = '''
        <div class="section">
            <div class="section-title">🏗️ 数据结构比较</div>
            <table>
                <tr><th>项目</th><th>实际结果</th><th>期望结果</th></tr>
                <tr><td>总列数</td><td>{}</td><td>{}</td></tr>
        '''.format(
            len(schema_comparison.get("actual_columns", [])),
            len(schema_comparison.get("expected_columns", []))
        )
        
        if schema_comparison.get("missing_columns"):
            html += f'<tr class="error"><td>缺失列</td><td colspan="2">{", ".join(schema_comparison["missing_columns"])}</td></tr>'
        
        if schema_comparison.get("extra_columns"):
            html += f'<tr class="warning"><td>额外列</td><td colspan="2">{", ".join(schema_comparison["extra_columns"])}</td></tr>'
        
        html += '</table>'
        
        if schema_comparison.get("data_type_mismatches"):
            html += '<h4>数据类型不匹配</h4><table><tr><th>列名</th><th>实际类型</th><th>期望类型</th></tr>'
            for col, types in schema_comparison["data_type_mismatches"].items():
                html += f'<tr class="error"><td>{col}</td><td>{types["actual"]}</td><td>{types["expected"]}</td></tr>'
            html += '</table>'
        
        html += '</div>'
        return html
    
    def _generate_content_section(self, content_comparison: Dict) -> str:
        """生成内容比较部分的HTML"""
        if not content_comparison:
            return ""
        
        html = f'''
        <div class="section">
            <div class="section-title">📊 数据内容比较</div>
            <table>
                <tr><th>指标</th><th>数值</th></tr>
                <tr><td>实际数据行数</td><td>{content_comparison.get("actual_row_count", 0)}</td></tr>
                <tr><td>期望数据行数</td><td>{content_comparison.get("expected_row_count", 0)}</td></tr>
                <tr><td>匹配行数</td><td>{content_comparison.get("matching_rows", 0)}</td></tr>
                <tr><td>匹配百分比</td><td>{content_comparison.get("data_match_percentage", 0):.2f}%</td></tr>
                <tr><td>仅存在于实际结果</td><td>{content_comparison.get("rows_only_in_actual", 0)}</td></tr>
                <tr><td>仅存在于期望结果</td><td>{content_comparison.get("rows_only_in_expected", 0)}</td></tr>
            </table>
        </div>
        '''
        return html
    
    def _generate_statistics_section(self, statistics: Dict) -> str:
        """生成统计信息部分的HTML"""
        if not statistics:
            return ""
        
        html = '<div class="section"><div class="section-title">📈 数据统计</div>'
        
        for table_name, stats in statistics.items():
            html += f'<h4>{table_name}</h4><table><tr><th>列名</th><th>数据类型</th><th>总数</th><th>非空数</th><th>唯一值数</th></tr>'
            for col, col_stats in stats.items():
                html += f'''<tr>
                    <td>{col}</td>
                    <td>{col_stats.get("data_type", "")}</td>
                    <td>{col_stats.get("total_count", "")}</td>
                    <td>{col_stats.get("non_null_count", "")}</td>
                    <td>{col_stats.get("distinct_count", "")}</td>
                </tr>'''
            html += '</table>'
        
        html += '</div>'
        return html
    
    def _generate_log_section(self) -> str:
        """生成执行日志部分的HTML"""
        html = '<div class="section"><div class="section-title">📝 执行日志</div>'
        for log_entry in self.execution_log:
            css_class = "log-error" if "[ERROR]" in log_entry else "log-info" if "[INFO]" in log_entry else ""
            html += f'<div class="log-entry {css_class}">{log_entry}</div>'
        html += '</div>'
        return html
    
    def close(self):
        """关闭Spark会话"""
        if hasattr(self, 'spark'):
            self.spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Spark SQL验证器 - 比对数据转换结果")
    parser.add_argument("--source", "-s", required=True, help="源数据CSV文件路径")
    parser.add_argument("--expected", "-e", required=True, help="期望结果CSV文件路径")
    parser.add_argument("--sql", "-q", required=True, help="Spark SQL语句或SQL文件路径")
    parser.add_argument("--output", "-o", default="validation_report.html", help="HTML报告输出路径")
    
    args = parser.parse_args()
    
    # 检查参数
    print(f"源数据文件: {args.source}")
    print(f"期望结果文件: {args.expected}")
    print(f"SQL参数: {args.sql}")
    print(f"输出文件: {args.output}")
    
    # 检查文件是否存在
    if not os.path.exists(args.source):
        print(f"错误: 源数据文件不存在: {args.source}")
        sys.exit(1)
    
    if not os.path.exists(args.expected):
        print(f"错误: 期望结果文件不存在: {args.expected}")
        sys.exit(1)
    
    # 创建验证器实例
    validator = None
    
    validator = SparkSQLValidator()
    
    # 判断SQL参数是文件还是SQL语句
    if os.path.exists(args.sql):
        # 从文件读取SQL
        sql_query = validator.read_sql_from_file(args.sql)
    else:
        # 直接使用SQL语句
        sql_query = args.sql
    
    # 执行验证
    results = validator.validate_transformation(
        source_file=args.source,
        expected_file=args.expected,
        spark_sql=sql_query
    )
    
    # 生成HTML报告
    validator.generate_html_report(results, args.output)
        
