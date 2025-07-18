import pandas as pd
import numpy as np
from datetime import datetime
import os

def compare_csv_files(file1_path, file2_path, output_path="comparison_report.html"):
    """
    比较两个CSV文件并生成HTML报告
    
    参数:
    file1_path: 第一个CSV文件路径
    file2_path: 第二个CSV文件路径
    output_path: 输出HTML报告的路径
    """
    
    try:
        # 读取CSV文件
        df1 = pd.read_csv(file1_path)
        df2 = pd.read_csv(file2_path)
        
        # 获取文件名
        file1_name = os.path.basename(file1_path)
        file2_name = os.path.basename(file2_path)
        
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
            'file2_only_cols': list(set(df2.columns) - set(df1.columns))
        }
        
        # 生成HTML报告
        html_content = generate_html_report(df1, df2, stats)
        
        # 写入HTML文件
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"比较报告已生成：{output_path}")
        
    except Exception as e:
        print(f"错误：{str(e)}")

def generate_html_report(df1, df2, stats):
    """生成HTML报告内容"""
    
    # 查找行差异
    different_rows = []
    same_rows = []
    
    # 获取共同列
    common_cols = stats['common_cols']
    
    if common_cols:
        # 创建用于比较的数据框（仅包含共同列）
        df1_common = df1[common_cols].copy()
        df2_common = df2[common_cols].copy()
        
        # 比较每一行
        max_rows = max(len(df1), len(df2))
        
        for i in range(max_rows):
            row_diff = []
            
            # 检查行是否存在
            row1_exists = i < len(df1)
            row2_exists = i < len(df2)
            
            if row1_exists and row2_exists:
                # 两个文件都有这一行
                row1_data = df1_common.iloc[i]
                row2_data = df2_common.iloc[i]
                
                # 比较每个字段
                is_same = True
                for col in common_cols:
                    val1 = row1_data[col] if col in row1_data else ''
                    val2 = row2_data[col] if col in row2_data else ''
                    
                    # 处理NaN值
                    if pd.isna(val1) and pd.isna(val2):
                        continue
                    elif pd.isna(val1) or pd.isna(val2) or val1 != val2:
                        is_same = False
                        row_diff.append({
                            'column': col,
                            'file1_value': str(val1) if not pd.isna(val1) else 'NaN',
                            'file2_value': str(val2) if not pd.isna(val2) else 'NaN'
                        })
                
                if is_same:
                    same_rows.append(i)
                else:
                    different_rows.append({
                        'row_index': i,
                        'differences': row_diff
                    })
            
            elif row1_exists and not row2_exists:
                # 只有文件1有这一行
                different_rows.append({
                    'row_index': i,
                    'differences': [{'column': '整行', 'file1_value': '存在', 'file2_value': '不存在'}]
                })
            
            elif not row1_exists and row2_exists:
                # 只有文件2有这一行
                different_rows.append({
                    'row_index': i,
                    'differences': [{'column': '整行', 'file1_value': '不存在', 'file2_value': '存在'}]
                })
    
    # 生成HTML
    html_template = f"""
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>CSV文件比较报告</title>
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
            <h1>CSV文件比较报告</h1>
            
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
                        <div class="stat-label">文件1列数</div>
                        <div class="stat-value">{stats['file1_cols']}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">文件2列数</div>
                        <div class="stat-value">{stats['file2_cols']}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">相同行数</div>
                        <div class="stat-value">{len(same_rows)}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">不同行数</div>
                        <div class="stat-value">{len(different_rows)}</div>
                    </div>
                </div>
            </div>
            
            <div class="section">
                <h2>列信息对比</h2>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px;">
                    <div>
                        <h3>共同列 ({len(stats['common_cols'])})</h3>
                        <div class="columns-list">
                            {' '.join([f'<span class="column-tag">{col}</span>' for col in stats['common_cols']])}
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
                {generate_differences_html(different_rows, stats)}
            </div>
            
            <div class="section">
                <h2>相同的行</h2>
                <div class="same-rows">
                    <p><strong>相同行的索引:</strong> {', '.join(map(str, same_rows[:50]))}{' ...' if len(same_rows) > 50 else ''}</p>
                    <p>共有 <strong>{len(same_rows)}</strong> 行数据完全相同</p>
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

def generate_differences_html(different_rows, stats):
    """生成差异部分的HTML"""
    if not different_rows:
        return '<div class="same-rows"><p>没有发现数据差异！</p></div>'
    
    html_parts = []
    
    # 限制显示的差异行数，避免报告过长
    max_display = 100
    displayed_rows = different_rows[:max_display]
    
    for diff in displayed_rows:
        row_idx = diff['row_index']
        differences = diff['differences']
        
        html_parts.append(f'<div class="diff-row">')
        html_parts.append(f'<div class="diff-header">行 {row_idx + 1} 的差异:</div>')
        
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
        # 执行比较
        compare_csv_files(file1_path, file2_path, "comparison_report.html")

