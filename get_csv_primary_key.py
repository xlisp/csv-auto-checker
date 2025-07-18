import pandas as pd
import itertools
from typing import List, Tuple, Optional

class CSVPrimaryKeyFinder:
    def __init__(self, csv_file: str):
        """
        初始化CSV主键查找器
        
        Args:
            csv_file: CSV文件路径
        """
        self.csv_file = csv_file
        self.df = None
        self.load_csv()
    
    def load_csv(self):
        """加载CSV文件"""
        try:
            self.df = pd.read_csv(self.csv_file)
            print(f"成功加载CSV文件: {self.csv_file}")
            print(f"数据形状: {self.df.shape}")
            print(f"列名: {list(self.df.columns)}")
        except Exception as e:
            print(f"加载CSV文件失败: {e}")
            return
    
    def find_single_column_keys(self) -> List[str]:
        """
        查找单列主键
        
        Returns:
            可能的单列主键列表
        """
        if self.df is None:
            return []
        
        single_keys = []
        
        for column in self.df.columns:
            # 检查是否有重复值
            if not self.df[column].duplicated().any():
                # 检查是否有空值
                if not self.df[column].isnull().any():
                    single_keys.append(column)
        
        return single_keys
    
    def find_composite_keys(self, max_columns: int = 3) -> List[Tuple[str, ...]]:
        """
        查找组合主键
        
        Args:
            max_columns: 最大组合列数，默认为3
            
        Returns:
            可能的组合主键列表
        """
        if self.df is None:
            return []
        
        composite_keys = []
        columns = list(self.df.columns)
        
        # 检查2到max_columns列的组合
        for r in range(2, min(max_columns + 1, len(columns) + 1)):
            for combo in itertools.combinations(columns, r):
                # 检查组合是否唯一
                if not self.df[list(combo)].duplicated().any():
                    # 检查是否有空值
                    if not self.df[list(combo)].isnull().any().any():
                        composite_keys.append(combo)
        
        return composite_keys
    
    def analyze_uniqueness(self, columns: List[str]) -> dict:
        """
        分析指定列的唯一性统计
        
        Args:
            columns: 要分析的列名列表
            
        Returns:
            包含唯一性统计的字典
        """
        if self.df is None:
            return {}
        
        stats = {}
        
        for column in columns:
            if column in self.df.columns:
                total_rows = len(self.df)
                unique_count = self.df[column].nunique()
                null_count = self.df[column].isnull().sum()
                duplicate_count = self.df[column].duplicated().sum()
                
                stats[column] = {
                    'total_rows': total_rows,
                    'unique_count': unique_count,
                    'null_count': null_count,
                    'duplicate_count': duplicate_count,
                    'uniqueness_ratio': unique_count / total_rows if total_rows > 0 else 0
                }
        
        return stats
    
    def find_all_primary_keys(self, max_composite_columns: int = 3) -> dict:
        """
        查找所有可能的主键
        
        Args:
            max_composite_columns: 组合主键最大列数
            
        Returns:
            包含所有主键信息的字典
        """
        if self.df is None:
            return {}
        
        result = {
            'single_column_keys': [],
            'composite_keys': [],
            'column_stats': {}
        }
        
        # 查找单列主键
        single_keys = self.find_single_column_keys()
        result['single_column_keys'] = single_keys
        
        # 查找组合主键
        composite_keys = self.find_composite_keys(max_composite_columns)
        result['composite_keys'] = composite_keys
        
        # 分析所有列的唯一性
        result['column_stats'] = self.analyze_uniqueness(list(self.df.columns))
        
        return result
    
    def print_results(self, results: dict):
        """打印分析结果"""
        print("\n" + "="*60)
        print("CSV主键分析结果")
        print("="*60)
        
        # 打印单列主键
        print("\n【单列主键】")
        if results['single_column_keys']:
            for key in results['single_column_keys']:
                print(f"  ✓ {key}")
        else:
            print("  未找到单列主键")
        
        # 打印组合主键
        print("\n【组合主键】")
        if results['composite_keys']:
            for key in results['composite_keys']:
                print(f"  ✓ {' + '.join(key)}")
        else:
            print("  未找到组合主键")
        
        # 打印列统计信息
        print("\n【列唯一性统计】")
        print(f"{'列名':<15} {'总行数':<8} {'唯一值':<8} {'空值':<6} {'重复值':<8} {'唯一性比例':<12}")
        print("-" * 70)
        
        for col, stats in results['column_stats'].items():
            print(f"{col:<15} {stats['total_rows']:<8} {stats['unique_count']:<8} "
                  f"{stats['null_count']:<6} {stats['duplicate_count']:<8} "
                  f"{stats['uniqueness_ratio']:<12.2%}")

def main():
    """主函数示例"""
    # 使用示例
    csv_file = input("请输入CSV文件路径: ").strip()
    
    if not csv_file:
        # 如果没有输入，使用示例数据
        print("创建示例数据进行演示...")
        sample_data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'david@example.com', 'eve@example.com'],
            'department': ['IT', 'HR', 'IT', 'Finance', 'HR'],
            'employee_code': ['E001', 'E002', 'E003', 'E004', 'E005']
        }
        
        df = pd.DataFrame(sample_data)
        csv_file = 'sample_data.csv'
        df.to_csv(csv_file, index=False)
        print(f"示例数据已保存到: {csv_file}")
    
    # 创建主键查找器实例
    finder = CSVPrimaryKeyFinder(csv_file)
    
    # 查找所有主键
    results = finder.find_all_primary_keys(max_composite_columns=3)
    
    # 打印结果
    finder.print_results(results)
    
    # 额外功能：手动检查特定列组合
    print("\n" + "="*60)
    print("手动检查特定列组合")
    print("="*60)
    
    while True:
        user_input = input("\n请输入要检查的列名（用逗号分隔，输入'quit'退出）: ").strip()
        
        if user_input.lower() == 'quit':
            break
        
        if user_input:
            columns = [col.strip() for col in user_input.split(',')]
            
            # 检查列是否存在
            valid_columns = [col for col in columns if col in finder.df.columns]
            invalid_columns = [col for col in columns if col not in finder.df.columns]
            
            if invalid_columns:
                print(f"警告: 以下列不存在: {invalid_columns}")
            
            if valid_columns:
                # 检查是否为主键
                is_unique = not finder.df[valid_columns].duplicated().any()
                has_nulls = finder.df[valid_columns].isnull().any().any()
                
                print(f"\n检查结果 - 列组合: {' + '.join(valid_columns)}")
                print(f"  是否唯一: {'是' if is_unique else '否'}")
                print(f"  是否有空值: {'是' if has_nulls else '否'}")
                print(f"  是否可作为主键: {'是' if is_unique and not has_nulls else '否'}")
                
                # 显示详细统计
                if len(valid_columns) == 1:
                    stats = finder.analyze_uniqueness(valid_columns)
                    col_stats = stats[valid_columns[0]]
                    print(f"  唯一值数量: {col_stats['unique_count']}")
                    print(f"  重复值数量: {col_stats['duplicate_count']}")
                    print(f"  唯一性比例: {col_stats['uniqueness_ratio']:.2%}")

if __name__ == "__main__":
    main()
