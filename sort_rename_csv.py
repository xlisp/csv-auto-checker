import pandas as pd
import os
from typing import Dict, List, Optional

class CSVFieldMapper:
    def __init__(self, input_file: str, output_file: str):
        """
        初始化CSV字段映射器
        
        Args:
            input_file: 输入CSV文件路径
            output_file: 输出CSV文件路径
        """
        self.input_file = input_file
        self.output_file = output_file
        self.df = None
        
    def read_csv(self, encoding: str = 'utf-8') -> pd.DataFrame:
        """
        读取CSV文件
        
        Args:
            encoding: 文件编码，默认utf-8
            
        Returns:
            pandas DataFrame
        """
        try:
            self.df = pd.read_csv(self.input_file, encoding=encoding)
            print(f"成功读取文件: {self.input_file}")
            print(f"原始列名: {list(self.df.columns)}")
            print(f"数据行数: {len(self.df)}")
            return self.df
        except UnicodeDecodeError:
            # 如果UTF-8编码失败，尝试GBK编码
            try:
                self.df = pd.read_csv(self.input_file, encoding='gbk')
                print(f"成功读取文件 (GBK编码): {self.input_file}")
                return self.df
            except Exception as e:
                print(f"读取文件失败: {e}")
                return None
        except Exception as e:
            print(f"读取文件失败: {e}")
            return None
    
    def map_fields(self, field_mapping: Dict[str, str]) -> None:
        """
        映射字段名称
        
        Args:
            field_mapping: 字段映射字典，格式 {"原字段名": "新字段名"}
        """
        if self.df is None:
            print("请先读取CSV文件")
            return
            
        # 只映射存在的字段
        existing_mapping = {old: new for old, new in field_mapping.items() if old in self.df.columns}
        
        if existing_mapping:
            self.df = self.df.rename(columns=existing_mapping)
            print(f"字段映射完成: {existing_mapping}")
        else:
            print("没有找到可映射的字段")
    
    def reorder_fields(self, new_order: List[str]) -> None:
        """
        重新排列字段顺序
        
        Args:
            new_order: 新的字段顺序列表
        """
        if self.df is None:
            print("请先读取CSV文件")
            return
            
        # 只选择存在的字段
        existing_fields = [field for field in new_order if field in self.df.columns]
        
        if existing_fields:
            # 重新排列现有字段，并添加不在new_order中的其他字段
            remaining_fields = [col for col in self.df.columns if col not in existing_fields]
            final_order = existing_fields + remaining_fields
            
            self.df = self.df[final_order]
            print(f"字段重新排列完成: {final_order}")
        else:
            print("没有找到可排列的字段")
    
    def add_or_modify_field(self, field_name: str, value, position: Optional[int] = None) -> None:
        """
        添加或修改字段
        
        Args:
            field_name: 字段名
            value: 字段值（可以是常量或基于其他字段的计算）
            position: 插入位置（可选）
        """
        if self.df is None:
            print("请先读取CSV文件")
            return
            
        self.df[field_name] = value
        
        # 如果指定了位置，重新排列字段
        if position is not None:
            cols = list(self.df.columns)
            cols.remove(field_name)
            cols.insert(position, field_name)
            self.df = self.df[cols]
            
        print(f"字段 '{field_name}' 添加/修改完成")
    
    def filter_and_select_fields(self, selected_fields: List[str]) -> None:
        """
        只保留指定的字段
        
        Args:
            selected_fields: 要保留的字段列表
        """
        if self.df is None:
            print("请先读取CSV文件")
            return
            
        existing_fields = [field for field in selected_fields if field in self.df.columns]
        
        if existing_fields:
            self.df = self.df[existing_fields]
            print(f"只保留字段: {existing_fields}")
        else:
            print("没有找到可保留的字段")
    
    def export_csv(self, encoding: str = 'utf-8-sig', index: bool = False) -> None:
        """
        导出CSV文件
        
        Args:
            encoding: 输出编码，默认utf-8-sig（Excel兼容）
            index: 是否包含索引
        """
        if self.df is None:
            print("没有数据可导出")
            return
            
        try:
            self.df.to_csv(self.output_file, encoding=encoding, index=index)
            print(f"文件导出成功: {self.output_file}")
            print(f"最终列名: {list(self.df.columns)}")
        except Exception as e:
            print(f"导出文件失败: {e}")
    
    def preview_data(self, rows: int = 5) -> None:
        """
        预览数据
        
        Args:
            rows: 预览行数
        """
        if self.df is None:
            print("请先读取CSV文件")
            return
            
        print(f"\n数据预览 (前{rows}行):")
        print(self.df.head(rows))
        print(f"\n数据信息:")
        print(self.df.info())


def main():
    """
    主函数 - 使用示例
    """
    # 配置文件路径
    input_file = "input.csv"  # 输入文件路径
    output_file = "output.csv"  # 输出文件路径
    
    # 创建映射器实例
    mapper = CSVFieldMapper(input_file, output_file)
    
    # 读取CSV文件
    if mapper.read_csv() is None:
        return
    
    # 配置字段映射（原字段名 -> 新字段名）
    field_mapping = {
        "name": "姓名",
        "age": "年龄", 
        "email": "邮箱",
        "phone": "电话",
        "address": "地址"
    }
    
    # 执行字段映射
    mapper.map_fields(field_mapping)
    
    # 配置新的字段顺序
    new_field_order = ["姓名", "年龄", "电话", "邮箱", "地址"]
    
    # 重新排列字段
    mapper.reorder_fields(new_field_order)
    
    # 可选：添加新字段
    # mapper.add_or_modify_field("创建时间", "2024-01-01", position=0)
    
    # 可选：只保留特定字段
    # mapper.filter_and_select_fields(["姓名", "年龄", "邮箱"])
    
    # 预览数据
    mapper.preview_data()
    
    # 导出CSV
    mapper.export_csv()


if __name__ == "__main__":
    main()


# 使用示例2：批量处理
def batch_process_example():
    """
    批量处理示例
    """
    # 配置
    configs = [
        {
            "input": "employees.csv",
            "output": "employees_processed.csv",
            "mapping": {"emp_id": "员工ID", "emp_name": "员工姓名", "dept": "部门"},
            "order": ["员工ID", "员工姓名", "部门"]
        },
        {
            "input": "products.csv", 
            "output": "products_processed.csv",
            "mapping": {"prod_id": "产品ID", "prod_name": "产品名称", "price": "价格"},
            "order": ["产品ID", "产品名称", "价格"]
        }
    ]
    
    for config in configs:
        print(f"\n处理文件: {config['input']}")
        mapper = CSVFieldMapper(config["input"], config["output"])
        
        if mapper.read_csv() is not None:
            mapper.map_fields(config["mapping"])
            mapper.reorder_fields(config["order"])
            mapper.export_csv()
            print(f"完成处理: {config['output']}")


# 使用示例3：高级功能
def advanced_example():
    """
    高级功能示例
    """
    mapper = CSVFieldMapper("sales_data.csv", "sales_processed.csv")
    
    if mapper.read_csv() is not None:
        # 字段映射
        mapper.map_fields({
            "sale_date": "销售日期",
            "product": "产品",
            "quantity": "数量",
            "unit_price": "单价"
        })
        
        # 添加计算字段
        mapper.add_or_modify_field("总金额", mapper.df["数量"] * mapper.df["单价"])
        
        # 添加常量字段
        mapper.add_or_modify_field("数据来源", "销售系统", position=0)
        
        # 重新排列
        mapper.reorder_fields(["数据来源", "销售日期", "产品", "数量", "单价", "总金额"])
        
        # 导出
        mapper.export_csv()

