# CSV 自动对比工具：输出相同部分，以及不同部分在哪里

## 设计思路
* 设计思路为基于主键(可组合主键)的对比，并生成html的report的界面显示哪部分不同, 如果不基于主键对比，搜索空间太大无法对比: `rg id1 | grep id2 ` 小部分的数据里面做比较就计算量很小
* 高性能对比大CSV的问题, 用go写高性能方案，或减少搜索范围，以此减少计算空间
* 可视化输出，可以关键输出比较不同的数据位置，快速迭代集成有效相同部分, 从而得到相应的transform是什么
* 引入Agent的设计做智能化的随机抽样式的比较，去引导减少对比计算量: 比如`rg id1 | grep id2 `的很少奇异值的比较
* 支持排除字段比较：逐个字段比较突破

---

设计一个基于主键的CSV自动对比工具。这个工具将生成HTML报告来显示数据差异，并包含高性能优化和智能抽样功能。

## 主要特性

### 1. **基于主键的高效对比**
- 支持单个或组合主键
- 创建主键索引以优化查找性能
- 只对比具有相同主键的记录

### 2. **高性能优化**
- 分块加载大CSV文件（避免内存溢出）
- 并行处理数据块（多线程）
- 智能抽样减少计算量

### 3. **智能抽样策略**
- 基于共同主键的抽样，而非完全随机
- 可配置抽样率（0.1 = 10%）
- 自动回退到随机抽样（当无共同主键时）

### 4. **可视化HTML报告**
- 详细的对比摘要统计
- 逐行显示字段差异
- 字段差异频率分析
- 建议的数据转换规则

### 5. **灵活的输出格式**
- HTML可视化报告
- CSV格式的差异数据导出
- 命令行摘要输出

## 使用示例

```bash
# 基本使用
python csv_compare.py file1.csv file2.csv --keys id

# 组合主键
python csv_compare.py file1.csv file2.csv --keys user_id product_id

# 设置抽样率和输出路径
python csv_compare.py file1.csv file2.csv --keys id --sample-rate 0.2 --output report.html

# 导出差异数据
python csv_compare.py file1.csv file2.csv --keys id --export-csv differences.csv
```

## 程序化使用

```python
from csv_compare import CSVComparator

# 创建对比器
comparator = CSVComparator(
    primary_keys=['id'],
    sample_rate=0.1
)

# 执行对比
results = comparator.compare_csvs('file1.csv', 'file2.csv')

# 导出差异
comparator.export_differences_csv('differences.csv')
```

## 性能优化策略

1. **内存管理**: 分块加载大文件
2. **并行处理**: 多线程处理数据块
3. **智能抽样**: 基于主键的有针对性抽样
4. **索引优化**: 使用pandas索引加速查找

这个工具特别适合处理大型CSV文件的对比，通过智能抽样和并行处理，可以在合理的时间内完成对比并生成直观的HTML报告。

## 生成演示图：

![./csv_compare.png]

---

## 支持排除字段比较：

```
坚持去λ化(中-易) csv-auto-checker  main @ python csv_compare_ignore.py  file1.csv file2.csv --keys user_id product_id --exclude quantity purchase_date
2025-07-19 06:24:24,896 - INFO - 开始CSV对比流程
2025-07-19 06:24:24,896 - INFO - 开始加载CSV文件: file1.csv
2025-07-19 06:24:24,900 - INFO - 成功加载 10 行数据
2025-07-19 06:24:24,900 - INFO - 开始加载CSV文件: file2.csv
2025-07-19 06:24:24,902 - INFO - 成功加载 10 行数据
2025-07-19 06:24:24,902 - INFO - 排除比较的字段: quantity, purchase_date
2025-07-19 06:24:24,903 - INFO - 文件1中的主键示例: ['1001|P001', '1001|P002', '1002|P001']
2025-07-19 06:24:24,903 - INFO - 文件2中的主键示例: ['1001|P001', '1001|P002', '1002|P001']
2025-07-19 06:24:24,903 - INFO - 找到 8 个共同主键
2025-07-19 06:24:24,904 - INFO - 智能抽样完成: 8 个主键, 8 + 8 行数据
2025-07-19 06:24:24,907 - INFO - 对比完成: 8 个共同主键
2025-07-19 06:24:24,908 - INFO - HTML报告已生成: comparison_report.html

=== 对比完成 ===
总主键数: 8
共同主键数: 8
相同行数: 5
差异行数: 3
HTML报告: comparison_report.html
坚持去λ化(中-易) csv-auto-checker  main @

```
