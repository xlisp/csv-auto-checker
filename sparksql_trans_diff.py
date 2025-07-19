#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV文件转换脚本
使用PySpark SQL将file1.csv转换为file2.csv格式
处理字段名映射和日期格式转换
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, when, isnan, isnull
from pyspark.sql.types import *
import sys
import os

class CSVTransformer:
    def __init__(self):
        # 初始化Spark会话
        self.spark = SparkSession.builder \
            .appName("CSV_Transformer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # 设置日志级别
        self.spark.sparkContext.setLogLevel("WARN")
    
    def define_field_mapping(self):
        """
        定义字段映射关系
        key: file1中的字段名
        value: file2中的字段名
        """
        return {
            'Userid': 'UID',
            'UserName': 'Name', 
            'CreateDate': 'Created_Date',
            'UpdateTime': 'Last_Updated',
            'Age': 'User_Age',
            'Email': 'Email_Address',
            'Status': 'Account_Status',
            'Amount': 'Transaction_Amount'
        }
    
    def define_date_format_mapping(self):
        """
        定义日期格式转换
        key: 字段名
        value: dict包含输入格式和输出格式
        """
        return {
            'CreateDate': {
                'input_format': 'yyyy-MM-dd HH:mm:ss',
                'output_format': 'yyyy/MM/dd'
            },
            'UpdateTime': {
                'input_format': 'dd-MM-yyyy',
                'output_format': 'yyyy-MM-dd HH:mm:ss'
            }
        }
    
    def read_csv(self, file_path, has_header=True):
        """读取CSV文件"""
        try:
            df = self.spark.read \
                .option("header", has_header) \
                .option("inferSchema", "true") \
                .option("encoding", "UTF-8") \
                .option("multiline", "true") \
                .option("escape", '"') \
                .csv(file_path)
            
            print(f"成功读取文件: {file_path}")
            print(f"数据行数: {df.count()}")
            print("原始数据schema:")
            df.printSchema()
            return df
        
        except Exception as e:
            print(f"读取文件失败 {file_path}: {str(e)}")
            return None
    
    def transform_data(self, df):
        """使用Spark SQL转换数据"""
        try:
            # 将DataFrame注册为临时视图
            df.createOrReplaceTempView("source_table")
            
            # 获取字段映射和日期格式映射
            field_mapping = self.define_field_mapping()
            date_format_mapping = self.define_date_format_mapping()
            
            # 构建SQL查询语句
            select_clauses = []
            
            for old_field, new_field in field_mapping.items():
                if old_field in df.columns:
                    # 检查是否需要日期格式转换
                    if old_field in date_format_mapping:
                        date_config = date_format_mapping[old_field]
                        input_fmt = date_config['input_format']
                        output_fmt = date_config['output_format']
                        
                        # 使用Spark SQL进行日期转换
                        clause = f"date_format(to_date({old_field}, '{input_fmt}'), '{output_fmt}') as {new_field}"
                    else:
                        # 普通字段映射
                        clause = f"{old_field} as {new_field}"
                    
                    select_clauses.append(clause)
                else:
                    print(f"警告: 字段 '{old_field}' 在源数据中不存在")
            
            # 添加其他可能需要的字段处理
            additional_fields = [
                # 示例：添加计算字段
                "current_timestamp() as Processing_Time",
                # 示例：条件字段
                "case when User_Age >= 18 then 'Adult' else 'Minor' end as Age_Group"
            ]
            
            # 如果源数据中有Age字段，才添加Age_Group
            if 'Age' in df.columns:
                select_clauses.extend(additional_fields)
            
            # 构建完整的SQL查询
            sql_query = f"""
            SELECT 
                {', '.join(select_clauses)}
            FROM source_table
            WHERE 1=1
                -- 添加数据过滤条件（可根据需要修改）
                AND {list(field_mapping.keys())[0]} IS NOT NULL
            """
            
            print("执行SQL查询:")
            print(sql_query)
            
            # 执行转换
            transformed_df = self.spark.sql(sql_query)
            
            # 数据质量检查
            print("\n转换后数据schema:")
            transformed_df.printSchema()
            print(f"转换后数据行数: {transformed_df.count()}")
            
            # 显示样本数据
            print("\n转换后数据样本:")
            transformed_df.show(5, truncate=False)
            
            return transformed_df
            
        except Exception as e:
            print(f"数据转换失败: {str(e)}")
            return None
    
    def apply_data_quality_rules(self, df):
        """应用数据质量规则"""
        try:
            # 注册临时视图
            df.createOrReplaceTempView("transform_table")
            
            # 使用SQL进行数据清洗
            quality_sql = """
            SELECT *,
                   -- 数据质量标记
                   CASE 
                       WHEN UID IS NULL THEN 'Missing_UID'
                       WHEN Name IS NULL OR Name = '' THEN 'Missing_Name'
                       WHEN Email_Address IS NULL OR Email_Address NOT RLIKE '^[^@]+@[^@]+\\.[^@]+$' THEN 'Invalid_Email'
                       ELSE 'Valid'
                   END as Data_Quality_Flag
            FROM transform_table
            """
            
            quality_df = self.spark.sql(quality_sql)
            
            # 统计数据质量
            print("\n数据质量统计:")
            quality_df.groupBy("Data_Quality_Flag").count().show()
            
            return quality_df
            
        except Exception as e:
            print(f"数据质量检查失败: {str(e)}")
            return df
    
    def write_csv(self, df, output_path, has_header=True):
        """写入CSV文件"""
        try:
            # 重新分区以优化输出
            df_output = df.coalesce(1)  # 合并为单个文件
            
            df_output.write \
                .mode("overwrite") \
                .option("header", has_header) \
                .option("encoding", "UTF-8") \
                .option("quote", '"') \
                .option("escape", '"') \
                .csv(output_path)
            
            print(f"成功写入文件到: {output_path}")
            
        except Exception as e:
            print(f"写入文件失败: {str(e)}")
    
    def run_transformation(self, input_path, output_path):
        """执行完整的转换流程"""
        print("开始CSV文件转换...")
        print(f"输入文件: {input_path}")
        print(f"输出路径: {output_path}")
        
        # 1. 读取源文件
        source_df = self.read_csv(input_path)
        if source_df is None:
            return False
        
        # 2. 转换数据
        transformed_df = self.transform_data(source_df)
        if transformed_df is None:
            return False
        
        # 3. 应用数据质量规则
        final_df = self.apply_data_quality_rules(transformed_df)
        
        # 4. 写入目标文件
        self.write_csv(final_df, output_path)
        
        print("转换完成!")
        return True
    
    def close(self):
        """关闭Spark会话"""
        self.spark.stop()

def main():
    # 配置文件路径
    input_file = "file1.csv"  # 修改为实际的输入文件路径
    output_path = "file2_output"  # 修改为实际的输出路径
    
    # 检查输入文件是否存在
    if not os.path.exists(input_file):
        print(f"错误: 输入文件 {input_file} 不存在")
        print("请创建示例文件或修改input_file路径")
        return
    
    # 创建转换器实例
    transformer = CSVTransformer()
    
    try:
        # 执行转换
        success = transformer.run_transformation(input_file, output_path)
        
        if success:
            print("\n=== 转换成功完成 ===")
        else:
            print("\n=== 转换失败 ===")
            
    except Exception as e:
        print(f"程序执行错误: {str(e)}")
        
    finally:
        # 关闭Spark会话
        transformer.close()

if __name__ == "__main__":
    main()