import pandas as pd
import numpy as np
from datetime import datetime
import os
import hashlib

def compare_csv_files(file1_path, file2_path, output_path="comparison_report.html", 
                     key_columns=None, ignore_order=True, normalize_values=True):
    """
    比较两个CSV文件并生成HTML报告
    
    参数:
    file1_path: 第一个CSV文件路径
    file2_path: 第二个CSV文件路径
    output_path: 输出HTML报告的路径
    key_columns: 用于匹配行的关键列列表，如果为None则自动检测
    ignore_order: 是否忽略行的顺序
    normalize_values: 是否标准化数值（如0和0.0视为相同）
    """
    
    try:
        # 读取CSV文件
        df1 = pd.read_csv(file1_path)
        df2 = pd.read_csv(file2_path)
        
        # 获取文件名
        file1_name = os.path.basename(file1_path)
        file2_name = os.path.basename(file2_path)
        
        # 数据预处理
        if normalize_values:
            df1 = normalize_dataframe(df1)
            df2 = normalize_dataframe(df2)
        
        # 基本信息统计
        stats = {
            'file1_name': file1_name,
            'file2_name': file2_name,
            'file1_rows': len(df1),
            'file2_rows': len(df2),
            'file1_cols': len(df1.columns),
            'file2_cols': len(df2.columns),
            'common_cols': list(set(df1.columns) & set(df2.columns)),
            'file1_only_cols': list(set(df1.columns) - set(df2.columns)),
            'file2_only_cols': list(set(df2.columns) - set(df1.columns)),
            'key_columns': key_columns,
            'ignore_order': ignore_order,
            'normalize_values': normalize_values
        }
        
        # 根据是否忽略顺序选择不同的比较方法
        if ignore_order:
            comparison_result = compare_with_matching(df1, df2, stats, key_columns)
        else:
            comparison_result = compare_row_by_row(df1, df2, stats)
        
        # 生成HTML报告
        html_content = generate_html_report(comparison_result, stats)
        
        # 写入HTML文件
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"比较报告已生成：{output_path}")
        print(f"匹配到的行数: {comparison_result['matched_rows']}")
        print(f"文件1独有的行数: {comparison_result['file1_only_rows']}")
        print(f"文件2独有的行数: {comparison_result['file2_only_rows']}")
        print(f"有差异的行数: {len(comparison_result['different_rows'])}")
        
    except Exception as e:
        print(f"错误：{str(e)}")
        import traceback
        traceback.print_exc()

def normalize_dataframe(df):
    """标准化数据框中的值"""
    df_normalized = df.copy()
    
    for col in df_normalized.columns:
        # 处理数值列
        if df_normalized[col].dtype in ['int64', 'float64', 'int32', 'float32']:
            # 将整数和浮点数标准化为统一格式
            df_normalized[col] = pd.to_numeric(df_normalized[col], errors='coerce')
        
        # 处理字符串列
        elif df_normalized[col].dtype == 'object':
            # 尝试转换为数值
            numeric_series = pd.to_numeric(df_normalized[col], errors='coerce')
            if not numeric_series.isna().all():
                # 如果大部分值都能转换为数值，则使用数值
                df_normalized[col] = numeric_series
            else:
                # 否则标准化字符串（去除前后空格，统一大小写）
                df_normalized[col] = df_normalized[col].astype(str).str.strip().str.lower()
    
    return df_normalized

def detect_key_columns(df1, df2, common_cols):
    """自动检测可能的关键列"""
    if not common_cols:
        return None
    
    # 寻找可能的ID列
    id_candidates = []
    for col in common_cols:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['id', 'key', 'index', 'code', 'number']):
            id_candidates.append(col)
    
    if id_candidates:
        return id_candidates[:2]  # 最多取前两个
    
    # 如果没有明显的ID列，选择唯一值较多的列
    uniqueness_scores = []
    for col in common_cols:
        if col in df1.columns and col in df2.columns:
            unique_ratio1 = df1[col].nunique() / len(df1)
            unique_ratio2 = df2[col].nunique() / len(df2)
            avg_uniqueness = (unique_ratio1 + unique_ratio2) / 2
            uniqueness_scores.append((col, avg_uniqueness))
    
    # 按唯一性排序，选择最独特的列
    uniqueness_scores.sort(key=lambda x: x[1], reverse=True)
    return [col for col, _ in uniqueness_scores[:2]]

def create_row_hash(row, columns):
    """为行创建哈希值用于匹配"""
    values = []
    for col in columns:
        if col in row.index:
            val = row[col]
            if pd.isna(val):
                values.append('NaN')
            else:
                values.append(str(val))
        else:
            values.append('MISSING')
    
    return hashlib.md5('|'.join(values).encode()).hexdigest()

def compare_with_matching(df1, df2, stats, key_columns=None):
    """使用行匹配的方式比较（忽略顺序）"""
    common_cols = stats['common_cols']
    
    if not common_cols:
        return {
            'matched_rows': 0,
            'file1_only_rows': len(df1),
            'file2_only_rows': len(df2),
            'different_rows': [],
            'same_rows': []
        }
    
    # 自动检测关键列
    if key_columns is None:
        key_columns = detect_key_columns(df1, df2, common_cols)
    
    if not key_columns:
        # 如果没有关键列，使用所有共同列创建哈希
        key_columns = common_cols
    
    # 为每行创建标识符
    df1_hashes = {}
    df2_hashes = {}
    
    for idx, row in df1.iterrows():
        row_hash = create_row_hash(row, key_columns)
        if row_hash not in df1_hashes:
            df1_hashes[row_hash] = []
        df1_hashes[row_hash].append((idx, row))
    
    for idx, row in df2.iterrows():
        row_hash = create_row_hash(row, key_columns)
        if row_hash not in df2_hashes:
            df2_hashes[row_hash] = []
        df2_hashes[row_hash].append((idx, row))
    
    # 找出匹配的行
    matched_rows = 0
    different_rows = []
    same_rows = []
    
    all_hashes = set(df1_hashes.keys()) | set(df2_hashes.keys())
    
    for row_hash in all_hashes:
        df1_rows = df1_hashes.get(row_hash, [])
        df2_rows = df2_hashes.get(row_hash, [])
        
        if df1_rows and df2_rows:
            # 有匹配的行
            matched_rows += min(len(df1_rows), len(df2_rows))
            
            # 比较第一对匹配的行
            idx1, row1 = df1_rows[0]
            idx2, row2 = df2_rows[0]
            
            # 比较所有共同列
            row_differences = []
            for col in common_cols:
                val1 = row1[col] if col in row1.index else np.nan
                val2 = row2[col] if col in row2.index else np.nan
                
                if not values_equal(val1, val2):
                    row_differences.append({
                        'column': col,
                        'file1_value': format_value(val1),
                        'file2_value': format_value(val2)
                    })
            
            if row_differences:
                different_rows.append({
                    'row_index': f"{idx1+1} ↔ {idx2+1}",
                    'key_values': {col: format_value(row1[col]) for col in key_columns if col in row1.index},
                    'differences': row_differences
                })
            else:
                same_rows.append(f"{idx1+1} ↔ {idx2+1}")
    
    file1_only_rows = sum(len(df1_hashes.get(h, [])) for h in df1_hashes.keys() if h not in df2_hashes)
    file2_only_rows = sum(len(df2_hashes.get(h, [])) for h in df2_hashes.keys() if h not in df1_hashes)
    
    return {
        'matched_rows': matched_rows,
        'file1_only_rows': file1_only_rows,
        'file2_only_rows': file2_only_rows,
        'different_rows': different_rows,
        'same_rows': same_rows,
        'key_columns': key_columns
    }

def compare_row_by_row(df1, df2, stats):
    """逐行比较（保持顺序）"""
    common_cols = stats['common_cols']
    different_rows = []
    same_rows = []
    
    if not common_cols:
        return {
            'matched_rows': 0,
            'file1_only_rows': len(df1),
            'file2_only_rows': len(df2),
            'different_rows': [],
            'same_rows': []
        }
    
    max_rows = max(len(df1), len(df2))
    matched_rows = min(len(df1), len(df2))
    
    for i in range(max_rows):
        row1_exists = i < len(df1)
        row2_exists = i < len(df2)
        
        if row1_exists and row2_exists:
            row1 = df1.iloc[i]
            row2 = df2.iloc[i]
            
            row_differences = []
            for col in common_cols:
                val1 = row1[col] if col in row1.index else np.nan
                val2 = row2[col] if col in row2.index else np.nan
                
                if not values_equal(val1, val2):
                    row_differences.append({
                        'column': col,
                        'file1_value': format_value(val1),
                        'file2_value': format_value(val2)
                    })
            
            if row_differences:
                different_rows.append({
                    'row_index': i + 1,
                    'differences': row_differences
                })
            else:
                same_rows.append(i + 1)
        
        elif row1_exists and not row2_exists:
            different_rows.append({
                'row_index': i + 1,
                'differences': [{'column': '整行', 'file1_value': '存在', 'file2_value': '不存在'}]
            })
        
        elif not row1_exists and row2_exists:
            different_rows.append({
                'row_index': i + 1,
                'differences': [{'column': '整行', 'file1_value': '不存在', 'file2_value': '存在'}]
            })
    
    return {
        'matched_rows': matched_rows,
        'file1_only_rows': len(df1) - matched_rows if len(df1) > matched_rows else 0,
        'file2_only_rows': len(df2) - matched_rows if len(df2) > matched_rows else 0,
        'different_rows': different_rows,
        'same_rows': same_rows
    }

def values_equal(val1, val2):
    """比较两个值是否相等（考虑数值等效性）"""
    # 处理NaN值
    if pd.isna(val1) and pd.isna(val2):
        return True
    elif pd.isna(val1) or pd.isna(val2):
        return False
    
    # 尝试数值比较
    try:
        num1 = float(val1)
        num2 = float(val2)
        return abs(num1 - num2) < 1e-10  # 使用小的容差处理浮点数精度问题
    except (ValueError, TypeError):
        # 如果不能转换为数值，则进行字符串比较
        return str(val1).strip().lower() == str(val2).strip().lower()

def format_value(val):
    """格式化值用于显示"""
    if pd.isna(val):
        return 'NaN'
    elif isinstance(val, (int, float)):
        # 对于数值，显示适当的格式
        if float(val).is_integer():
            return str(int(val))
        else:
            return f"{val:.6g}"  # 显示最多6位有效数字
    else:
        return str(val)

def generate_html_report(comparison_result, stats):
    """生成HTML报告内容"""
    
    html_template = f"""
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>CSV文件智能比较报告</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
                background-color: white;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            h1 {{
                color: #333;
                text-align: center;
                margin-bottom: 30px;
            }}
            .summary {{
                background-color: #f8f9fa;
                padding: 20px;
                border-radius: 5px;
                margin-bottom: 30px;
            }}
            .summary-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 15px;
            }}
            .stat-item {{
                background-color: white;
                padding: 15px;
                border-radius: 5px;
                border-left: 4px solid #007bff;
            }}
            .stat-label {{
                font-weight: bold;
                color: #666;
                font-size: 14px;
            }}
            .stat-value {{
                font-size: 18px;
                color: #333;
                margin-top: 5px;
            }}
            .config-info {{
                background-color: #e3f2fd;
                padding: 15px;
                border-radius: 5px;
                margin-bottom: 20px;
                border-left: 4px solid #2196f3;
            }}
            .section {{
                margin-bottom: 40px;
            }}
            .section h2 {{
                color: #333;
                border-bottom: 2px solid #007bff;
                padding-bottom: 10px;
            }}
            .differences {{
                background-color: #fff5f5;
                border: 1px solid #fed7d7;
                border-radius: 5px;
                padding: 15px;
                margin-bottom: 15px;
            }}
            .same-rows {{
                background-color: #f0fff4;
                border: 1px solid #c6f6d5;
                border-radius: 5px;
                padding: 15px;
            }}
            .diff-row {{
                background-color: white;
                border: 1px solid #e2e8f0;
                border-radius: 5px;
                padding: 15px;
                margin-bottom: 15px;
            }}
            .diff-header {{
                font-weight: bold;
                color: #e53e3e;
                margin-bottom: 10px;
            }}
            .key-info {{
                background-color: #fff3cd;
                padding: 10px;
                border-radius: 3px;
                margin-bottom: 10px;
                font-size: 14px;
            }}
            .diff-table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 10px;
            }}
            .diff-table th, .diff-table td {{
                border: 1px solid #e2e8f0;
                padding: 8px;
                text-align: left;
            }}
            .diff-table th {{
                background-color: #f7fafc;
                font-weight: bold;
            }}
            .file1-value {{
                background-color: #fed7d7;
                padding: 2px 6px;
                border-radius: 3px;
            }}
            .file2-value {{
                background-color: #bee3f8;
                padding: 2px 6px;
                border-radius: 3px;
            }}
            .columns-list {{
                display: flex;
                flex-wrap: wrap;
                gap: 10px;
            }}
            .column-tag {{
                background-color: #e2e8f0;
                padding: 5px 10px;
                border-radius: 15px;
                font-size: 12px;
            }}
            .key-column-tag {{
                background-color: #ffd700;
                padding: 5px 10px;
                border-radius: 15px;
                font-size: 12px;
                font-weight: bold;
            }}
            .timestamp {{
                text-align: center;
                color: #666;
                font-size: 14px;
                margin-top: 30px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>CSV文件智能比较报告</h1>
            
            <div class="config-info">
                <h3>比较配置</h3>
                <p><strong>忽略行顺序:</strong> {'是' if stats['ignore_order'] else '否'}</p>
                <p><strong>数值标准化:</strong> {'是（0和0.0视为相同）' if stats['normalize_values'] else '否'}</p>
                {f"<p><strong>匹配关键列:</strong> {', '.join(comparison_result.get('key_columns', []))}</p>" if comparison_result.get('key_columns') else ""}
            </div>
            
            <div class="summary">
                <h2>文件信息概览</h2>
                <div class="summary-grid">
                    <div class="stat-item">
                        <div class="stat-label">文件1</div>
                        <div class="stat-value">{stats['file1_name']}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">文件2</div>
                        <div class="stat-value">{stats['file2_name']}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">文件1行数</div>
                        <div class="stat-value">{stats['file1_rows']}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">文件2行数</div>
                        <div class="stat-value">{stats['file2_rows']}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">匹配行数</div>
                        <div class="stat-value">{comparison_result['matched_rows']}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">文件1独有行数</div>
                        <div class="stat-value">{comparison_result['file1_only_rows']}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">文件2独有行数</div>
                        <div class="stat-value">{comparison_result['file2_only_rows']}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">有差异的行数</div>
                        <div class="stat-value">{len(comparison_result['different_rows'])}</div>
                    </div>
                </div>
            </div>
            
            <div class="section">
                <h2>列信息对比</h2>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px;">
                    <div>
                        <h3>共同列 ({len(stats['common_cols'])})</h3>
                        <div class="columns-list">
                            {generate_column_tags(stats['common_cols'], comparison_result.get('key_columns', []))}
                        </div>
                    </div>
                    <div>
                        <h3>仅文件1有的列 ({len(stats['file1_only_cols'])})</h3>
                        <div class="columns-list">
                            {' '.join([f'<span class="column-tag" style="background-color: #fed7d7;">{col}</span>' for col in stats['file1_only_cols']])}
                        </div>
                    </div>
                    <div>
                        <h3>仅文件2有的列 ({len(stats['file2_only_cols'])})</h3>
                        <div class="columns-list">
                            {' '.join([f'<span class="column-tag" style="background-color: #bee3f8;">{col}</span>' for col in stats['file2_only_cols']])}
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="section">
                <h2>数据行差异详情</h2>
                {generate_differences_html(comparison_result['different_rows'], stats['ignore_order'])}
            </div>
            
            <div class="section">
                <h2>相同的行</h2>
                <div class="same-rows">
                    <p><strong>相同行:</strong> {', '.join(map(str, comparison_result['same_rows'][:50]))}{' ...' if len(comparison_result['same_rows']) > 50 else ''}</p>
                    <p>共有 <strong>{len(comparison_result['same_rows'])}</strong> 行数据完全相同</p>
                </div>
            </div>
            
            <div class="timestamp">
                报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            </div>
        </div>
    </body>
    </html>
    """
    
    return html_template

def generate_column_tags(common_cols, key_columns):
    """生成列标签HTML"""
    tags = []
    for col in common_cols:
        if col in key_columns:
            tags.append(f'<span class="key-column-tag">{col} (匹配键)</span>')
        else:
            tags.append(f'<span class="column-tag">{col}</span>')
    return ' '.join(tags)

def generate_differences_html(different_rows, ignore_order):
    """生成差异部分的HTML"""
    if not different_rows:
        return '<div class="same-rows"><p>没有发现数据差异！</p></div>'
    
    html_parts = []
    
    # 限制显示的差异行数
    max_display = 100
    displayed_rows = different_rows[:max_display]
    
    for diff in displayed_rows:
        row_idx = diff['row_index']
        differences = diff['differences']
        
        html_parts.append(f'<div class="diff-row">')
        
        if ignore_order:
            html_parts.append(f'<div class="diff-header">匹配行 {row_idx} 的差异:</div>')
            if 'key_values' in diff:
                key_info = ', '.join([f"{k}: {v}" for k, v in diff['key_values'].items()])
                html_parts.append(f'<div class="key-info">匹配关键值: {key_info}</div>')
        else:
            html_parts.append(f'<div class="diff-header">行 {row_idx} 的差异:</div>')
        
        if len(differences) == 1 and differences[0]['column'] == '整行':
            # 整行差异
            html_parts.append(f'<p>{differences[0]["file1_value"]} vs {differences[0]["file2_value"]}</p>')
        else:
            # 字段差异
            html_parts.append('<table class="diff-table">')
            html_parts.append('<tr><th>列名</th><th>文件1值</th><th>文件2值</th></tr>')
            
            for d in differences:
                html_parts.append(f'''
                <tr>
                    <td>{d['column']}</td>
                    <td><span class="file1-value">{d['file1_value']}</span></td>
                    <td><span class="file2-value">{d['file2_value']}</span></td>
                </tr>
                ''')
            
            html_parts.append('</table>')
        
        html_parts.append('</div>')
    
    if len(different_rows) > max_display:
        html_parts.append(f'<div class="diff-row"><p><strong>注意:</strong> 共有 {len(different_rows)} 行存在差异，此处仅显示前 {max_display} 行</p></div>')
    
    return ''.join(html_parts)

# 使用示例
if __name__ == "__main__":
    # 示例用法
    file1_path = "file1.csv"  # 替换为你的第一个CSV文件路径
    file2_path = "file2.csv"  # 替换为你的第二个CSV文件路径
    
    # 检查文件是否存在
    if not os.path.exists(file1_path):
        print(f"错误：找不到文件 {file1_path}")
        print("请确保文件路径正确，或者创建示例文件进行测试")
    elif not os.path.exists(file2_path):
        print(f"错误：找不到文件 {file2_path}")
        print("请确保文件路径正确，或者创建示例文件进行测试")
    else:
        # 执行比较 - 智能模式（忽略顺序，数值标准化）
        print("=== 智能比较模式 ===")
        compare_csv_files(
            file1_path, 
            file2_path, 
            "smart_comparison_report.html",
            key_columns=None,  # 自动检测关键列
            ignore_order=True,  # 忽略行顺序
            normalize_values=True  # 数值标准化
        )
        
        # 也可以指定关键列进行比较
        # compare_csv_files(
        #     file1_path, 
        #     file2_path, 
        #     "custom_comparison_report.html",
        #     key_columns=['id', 'name'],  # 指定用于匹配的列
        #     ignore_order=True,
        #     normalize_values=True
        # )
        
        # 或者使用传统的逐行比较模式
        # print("\n=== 传统比较模式 ===")
        # compare_csv_files(
        #     file1_path, 
        #     file2_path, 
        #     "traditional_comparison_report.html",
        #     ignore_order=False,  # 不忽略行顺序
        #     normalize_values=True
        # )

