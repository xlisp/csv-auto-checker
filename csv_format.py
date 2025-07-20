import pandas as pd
import numpy as np
from datetime import datetime
import os

class CSVProcessor:
    def __init__(self):
        self.df = None
        
    def generate_test_data(self, filename='test_data.csv', rows=100):
        """生成测试CSV数据"""
        print("正在生成测试数据...")
        
        # 生成测试数据
        data = {
            '日期': [f"202507{str(i%30+1).zfill(2)}" for i in range(rows)],
            '精度值': [round(1 + np.random.random() * 2, 3) for _ in range(rows)],
            '状态': ['A' if i % 3 == 0 else 'B' if i % 3 == 1 else 'C' for i in range(rows)],
            '名称': [f"产品_{i+1}" for i in range(rows)],
            '数量': [np.random.randint(10, 100) for _ in range(rows)],
            '价格': [round(np.random.uniform(50, 500), 2) for _ in range(rows)]
        }
        
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"测试数据已生成: {filename}")
        return filename
    
    def load_csv(self, filename):
        """加载CSV文件"""
        try:
            self.df = pd.read_csv(filename, encoding='utf-8-sig')
            print(f"成功加载文件: {filename}")
            print(f"数据形状: {self.df.shape}")
            print("列名:", list(self.df.columns))
            return True
        except Exception as e:
            print(f"加载文件失败: {e}")
            return False
    
    def convert_date_format(self, column_name, input_format='%Y%m%d', output_format='%Y-%m-%d'):
        """
        转换日期格式
        默认从 20250720 转换为 2025-07-20
        """
        if column_name not in self.df.columns:
            print(f"列 '{column_name}' 不存在")
            return False
            
        print(f"正在转换日期格式: {column_name}")
        try:
            # 转换为字符串确保格式一致
            self.df[column_name] = self.df[column_name].astype(str)
            
            # 转换日期格式
            self.df[column_name] = pd.to_datetime(self.df[column_name], format=input_format).dt.strftime(output_format)
            print(f"日期格式转换完成: {column_name}")
            return True
        except Exception as e:
            print(f"日期转换失败: {e}")
            return False
    
    def convert_precision(self, column_name, decimal_places=1):
        """
        转换精度，如 1.099 -> 1.1
        """
        if column_name not in self.df.columns:
            print(f"列 '{column_name}' 不存在")
            return False
            
        print(f"正在转换精度: {column_name} -> {decimal_places}位小数")
        try:
            # 转换为数值类型并保留指定小数位
            self.df[column_name] = pd.to_numeric(self.df[column_name], errors='coerce')
            self.df[column_name] = self.df[column_name].round(decimal_places)
            print(f"精度转换完成: {column_name}")
            return True
        except Exception as e:
            print(f"精度转换失败: {e}")
            return False
    
    def replace_values(self, column_name, value_mapping):
        """
        替换指定值，如 A -> B
        value_mapping: dict, 如 {'A': 'B', 'C': 'D'}
        """
        if column_name not in self.df.columns:
            print(f"列 '{column_name}' 不存在")
            return False
            
        print(f"正在替换值: {column_name}")
        try:
            # 使用replace方法替换值
            self.df[column_name] = self.df[column_name].replace(value_mapping)
            print(f"值替换完成: {column_name}")
            print(f"替换规则: {value_mapping}")
            return True
        except Exception as e:
            print(f"值替换失败: {e}")
            return False
    
    def preview_data(self, n=5):
        """预览数据"""
        if self.df is not None:
            print(f"\n数据预览 (前{n}行):")
            print(self.df.head(n))
            print(f"\n数据类型:")
            print(self.df.dtypes)
        else:
            print("没有加载数据")
    
    def export_csv(self, output_filename):
        """导出处理后的CSV"""
        if self.df is not None:
            try:
                self.df.to_csv(output_filename, index=False, encoding='utf-8-sig')
                print(f"文件已导出: {output_filename}")
                return True
            except Exception as e:
                print(f"导出失败: {e}")
                return False
        else:
            print("没有数据可导出")
            return False

def main():
    # 创建处理器实例
    processor = CSVProcessor()
    
    # 1. 生成测试数
    test_file = processor.generate_test_data('test_input.csv', 50)
    
    # 2. 加载CSV文件
    if not processor.load_csv(test_file):
        return
    
    print("\n=== 原始数据 ===")
    processor.preview_data()
    
    # 3. 执行各种转换
    print("\n=== 开始数据转换 ===")
    
    # 日期格式转换: 20250720 -> 2025-07-20
    processor.convert_date_format('日期')
    
    # 精度转换: 1.099 -> 1.1
    processor.convert_precision('精度值', decimal_places=1)
    
    # 值替换: A -> 优秀, B -> 良好, C -> 一般
    value_mapping = {'A': '优秀', 'B': '良好', 'C': '一般'}
    processor.replace_values('状态', value_mapping)
    
    print("\n=== 转换后数据 ===")
    processor.preview_data()
    
    # 4. 导出处理后的数据
    processor.export_csv('processed_output.csv')
    
    print("\n=== 处理完成 ===")
    print("输入文件: test_input.csv")
    print("输出文件: processed_output.csv")

# 使用示例
if __name__ == "__main__":
    # 运行主程序
    #main()
    
    # 创建新的处理器实例进行演示
    demo_processor = CSVProcessor()
    
    demo_processor.load_csv('demo_data.csv')
    
    # 单独执行转换操作
    demo_processor.convert_date_format('日期', '%Y%m%d', '%Y/%m/%d')  # 转换为 2025/07/20 格式
    demo_processor.convert_precision('精度值', 2)  # 保留2位小数
    demo_processor.replace_values('状态', {'A': 'Alpha', 'B': 'Beta', 'C': 'Gamma'})
    
    demo_processor.preview_data(3)
    demo_processor.export_csv('demo_output_new.csv')

