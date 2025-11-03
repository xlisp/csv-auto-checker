已完成修改！主要新增功能包括：

## 🎯 核心改进

### 1. **科学表示法识别**
- 自动识别 `1.23e-5`、`1E+10` 等科学表示法格式
- 同时支持普通小数格式

### 2. **智能数值比较**
- 使用相对误差容差（默认 1e-9）
- 特殊处理零值情况
- 公式：`|val1 - val2| / max(|val1|, |val2|) ≤ tolerance`

### 3. **增强的显示功能**
- 科学表示法自动转换为标准格式显示
- 大数值（>1e6）或小数值（<1e-4）自动添加科学表示法
- 示例：`1.23e-5 (= 1.23e-05)` 或 `1234567 (= 1.234567e+06)`

### 4. **详细的统计信息**
- 区分普通字段和数值字段差异
- HTML报告中用蓝色标记数值比较
- 导出CSV包含数值标识和标准化数值

## 📝 使用示例

```bash
# 基本用法（启用数值比较）
java CSVComparator file1.csv file2.csv --keys ID --numeric

# 自定义容差
java CSVComparator data1.csv data2.csv -k ID --numeric --tolerance 0.001

# 排除字段 + 数值比较
java CSVComparator old.csv new.csv -k ID -e Timestamp UpdateTime --numeric -t 1e-6

# 完整功能
java CSVComparator a.csv b.csv -k ID Name \
  --numeric --tolerance 0.0001 \
  --exclude CreatedAt ModifiedAt \
  --output report.html \
  --export-csv differences.csv
```

## 🆕 新增命令行参数

- `--numeric`: 启用数值比较（包括科学表示法）
- `--tolerance, -t <值>`: 设置相对误差容差（默认 1e-9）

程序会自动识别数值字段并应用合适的比较策略！

