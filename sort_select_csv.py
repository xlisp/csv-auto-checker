import pandas as pd
import sys
import os

def reorder_csv_columns(input_file, output_file, column_order):
    """
    根据指定的字段顺序重新排列CSV文件的列
    
    参数:
    input_file: 输入CSV文件路径
    output_file: 输出CSV文件路径
    column_order: 字段顺序列表
    """
    try:
        # 读取CSV文件
        df = pd.read_csv(input_file, encoding='utf-8')
        print(f"原始CSV文件包含 {len(df)} 行数据")
        print(f"原始字段: {list(df.columns)}")
        
        # 检查指定的字段是否存在
        missing_columns = [col for col in column_order if col not in df.columns]
        if missing_columns:
            print(f"警告: 以下字段在原始文件中不存在: {missing_columns}")
        
        # 筛选存在的字段
        valid_columns = [col for col in column_order if col in df.columns]
        if not valid_columns:
            print("错误: 没有找到任何有效的字段")
            return False
        
        # 按新顺序重排列
        df_reordered = df[valid_columns]
        print(f"重排后字段: {list(df_reordered.columns)}")
        
        # 导出新的CSV文件
        df_reordered.to_csv(output_file, index=False, encoding='utf-8')
        print(f"成功导出到: {output_file}")
        print(f"导出数据包含 {len(df_reordered)} 行, {len(df_reordered.columns)} 列")
        
        return True
        
    except FileNotFoundError:
        print(f"错误: 找不到文件 {input_file}")
        return False
    except Exception as e:
        print(f"处理过程中发生错误: {str(e)}")
        return False

def main():
    print("CSV字段重排序工具")
    print("=" * 50)
    
    # 获取输入文件路径
    input_file = input("请输入CSV文件路径: ").strip()
    
    # 检查文件是否存在
    if not os.path.exists(input_file):
        print("错误: 文件不存在")
        return
    
    # 先读取文件显示现有字段
    try:
        df_preview = pd.read_csv(input_file, nrows=0)  # 只读取表头
        print(f"\n当前文件包含以下字段:")
        for i, col in enumerate(df_preview.columns, 1):
            print(f"{i}. {col}")
    except Exception as e:
        print(f"读取文件时出错: {str(e)}")
        return
    
    print("\n请输入新的字段顺序:")
    print("方式1: 直接输入字段名，用逗号分隔")
    print("方式2: 输入字段编号，用逗号分隔")
    print("示例: name,age,city 或 1,3,2")
    
    field_input = input("字段顺序: ").strip()
    
    # 解析字段顺序
    if field_input.replace(',', '').replace(' ', '').isdigit():
        # 按编号输入
        try:
            indices = [int(x.strip()) - 1 for x in field_input.split(',')]
            column_order = [df_preview.columns[i] for i in indices if 0 <= i < len(df_preview.columns)]
        except (ValueError, IndexError):
            print("错误: 字段编号无效")
            return
    else:
        # 按字段名输入
        column_order = [col.strip() for col in field_input.split(',')]
    
    # 获取输出文件路径
    output_file = input("请输入输出文件路径 (按Enter使用默认): ").strip()
    if not output_file:
        base_name = os.path.splitext(input_file)[0]
        output_file = f"{base_name}_reordered.csv"
    
    # 执行重排序
    print(f"\n开始处理...")
    success = reorder_csv_columns(input_file, output_file, column_order)
    
    if success:
        print("处理完成!")
    else:
        print("处理失败!")

# 命令行参数版本
def cli_version():
    """
    命令行版本，支持直接传参
    用法: python script.py input.csv output.csv "col1,col2,col3"
    """
    if len(sys.argv) == 4:
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        column_order = [col.strip() for col in sys.argv[3].split(',')]
        
        print(f"输入文件: {input_file}")
        print(f"输出文件: {output_file}")
        print(f"字段顺序: {column_order}")
        
        success = reorder_csv_columns(input_file, output_file, column_order)
        if success:
            print("处理完成!")
        else:
            print("处理失败!")
    else:
        main()

if __name__ == "__main__":
    # 检查是否有命令行参数
    if len(sys.argv) > 1:
        cli_version()
    else:
        main()

