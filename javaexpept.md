下面是Java版本的CSV对比工具，实现了类似Spark `exceptAll` 的功能：

```java
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * CSV自动对比工具 - Java版本
 * 实现类似Spark exceptAll的功能
 */
public class CSVComparator {

    private final Set<String> excludeFields;
    private final double sampleRate;
    private ComparisonResult comparisonResult;

    public CSVComparator(Set<String> excludeFields, double sampleRate) {
        this.excludeFields = excludeFields != null ? excludeFields : new HashSet<>();
        this.sampleRate = sampleRate;
    }

    /**
     * CSV行数据类
     */
    static class CSVRow {
        private final Map<String, String> data;
        private final String hash;
        private final int originalIndex;

        public CSVRow(Map<String, String> data, String hash, int originalIndex) {
            this.data = data;
            this.hash = hash;
            this.originalIndex = originalIndex;
        }

        public Map<String, String> getData() {
            return data;
        }

        public String getHash() {
            return hash;
        }

        public int getOriginalIndex() {
            return originalIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CSVRow csvRow = (CSVRow) o;
            return hash.equals(csvRow.hash);
        }

        @Override
        public int hashCode() {
            return hash.hashCode();
        }
    }

    /**
     * 对比结果类
     */
    static class ComparisonResult {
        private final long totalRowsFile1;
        private final long totalRowsFile2;
        private final long identicalRows;
        private final List<CSVRow> onlyInFile1;
        private final List<CSVRow> onlyInFile2;
        private final List<String> columns;

        public ComparisonResult(long totalRowsFile1, long totalRowsFile2,
                                long identicalRows, List<CSVRow> onlyInFile1,
                                List<CSVRow> onlyInFile2, List<String> columns) {
            this.totalRowsFile1 = totalRowsFile1;
            this.totalRowsFile2 = totalRowsFile2;
            this.identicalRows = identicalRows;
            this.onlyInFile1 = onlyInFile1;
            this.onlyInFile2 = onlyInFile2;
            this.columns = columns;
        }

        public long getTotalRowsFile1() {
            return totalRowsFile1;
        }

        public long getTotalRowsFile2() {
            return totalRowsFile2;
        }

        public long getIdenticalRows() {
            return identicalRows;
        }

        public List<CSVRow> getOnlyInFile1() {
            return onlyInFile1;
        }

        public List<CSVRow> getOnlyInFile2() {
            return onlyInFile2;
        }

        public List<String> getColumns() {
            return columns;
        }
    }

    /**
     * 加载CSV文件
     */
    public List<CSVRow> loadCSV(String filePath) throws IOException, CsvException {
        System.out.println("开始加载CSV文件: " + filePath);

        List<CSVRow> rows = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new InputStreamReader(
                new FileInputStream(filePath), StandardCharsets.UTF_8))) {

            List<String[]> allRows = reader.readAll();

            if (allRows.isEmpty()) {
                throw new IllegalArgumentException("CSV文件为空");
            }

            // 第一行为表头
            String[] headers = allRows.get(0);
            List<String> headerList = Arrays.asList(headers);

            // 过滤掉排除的列
            List<Integer> includeIndices = new ArrayList<>();
            List<String> includeHeaders = new ArrayList<>();

            for (int i = 0; i < headers.length; i++) {
                if (!excludeFields.contains(headers[i])) {
                    includeIndices.add(i);
                    includeHeaders.add(headers[i]);
                }
            }

            System.out.println("列数: " + headers.length);
            System.out.println("列名: " + String.join(", ", headers));

            if (!excludeFields.isEmpty()) {
                System.out.println("排除字段: " + String.join(", ", excludeFields));
                System.out.println("比较字段: " + String.join(", ", includeHeaders));
            }

            // 处理数据行
            for (int i = 1; i < allRows.size(); i++) {
                String[] row = allRows.get(i);

                // 构建数据Map
                Map<String, String> rowData = new HashMap<>();
                StringBuilder hashBuilder = new StringBuilder();

                for (int idx : includeIndices) {
                    if (idx < row.length) {
                        String value = row[idx] != null ? row[idx] : "";
                        rowData.put(headers[idx], value);
                        hashBuilder.append(value).append("|");
                    }
                }

                // 计算行哈希
                String hash = calculateMD5(hashBuilder.toString());
                rows.add(new CSVRow(rowData, hash, i));
            }

            System.out.println("成功加载 " + rows.size() + " 行数据");
        }

        return rows;
    }

    /**
     * 计算MD5哈希
     */
    private String calculateMD5(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hashBytes = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5算法不可用", e);
        }
    }

    /**
     * 实现类似 Spark exceptAll 的功能
     * 返回在list1中但不在list2中的元素（保留重复）
     */
    public List<CSVRow> exceptAll(List<CSVRow> list1, List<CSVRow> list2) {
        System.out.println("执行 exceptAll 操作...");

        // 使用Map统计每个哈希在list2中出现的次数
        Map<String, Integer> list2Counts = new HashMap<>();
        for (CSVRow row : list2) {
            list2Counts.put(row.getHash(), list2Counts.getOrDefault(row.getHash(), 0) + 1);
        }

        // 使用Map统计每个哈希在list1中出现的次数
        Map<String, Integer> list1Counts = new HashMap<>();
        for (CSVRow row : list1) {
            list1Counts.put(row.getHash(), list1Counts.getOrDefault(row.getHash(), 0) + 1);
        }

        // 计算差异
        List<CSVRow> result = new ArrayList<>();
        Map<String, Integer> processedCounts = new HashMap<>();

        for (CSVRow row : list1) {
            String hash = row.getHash();
            int countInList1 = list1Counts.get(hash);
            int countInList2 = list2Counts.getOrDefault(hash, 0);
            int processed = processedCounts.getOrDefault(hash, 0);

            // 如果list1中该哈希的数量大于list2中的数量，则有差异
            if (processed < (countInList1 - countInList2)) {
                result.add(row);
                processedCounts.put(hash, processed + 1);
            }
        }

        return result;
    }

    /**
     * 智能抽样
     */
    public List<CSVRow> applySampling(List<CSVRow> rows) {
        if (sampleRate >= 1.0) {
            return rows;
        }

        int sampleSize = (int) (rows.size() * sampleRate);
        System.out.println("执行抽样，抽样率: " + sampleRate + ", 抽样数量: " + sampleSize);

        List<CSVRow> sampled = new ArrayList<>(rows);
        Collections.shuffle(sampled, new Random(42));
        return sampled.subList(0, Math.min(sampleSize, sampled.size()));
    }

    /**
     * 对比两个CSV文件
     */
    public ComparisonResult compareCSVs(String file1Path, String file2Path) 
            throws IOException, CsvException {
        
        System.out.println("=" .repeat(60));
        System.out.println("开始CSV对比流程");
        System.out.println("=" .repeat(60));

        // 加载数据
        List<CSVRow> rows1 = loadCSV(file1Path);
        List<CSVRow> rows2 = loadCSV(file2Path);

        long totalRows1 = rows1.size();
        long totalRows2 = rows2.size();

        // 应用抽样
        if (sampleRate < 1.0) {
            rows1 = applySampling(rows1);
            rows2 = applySampling(rows2);
        }

        // 使用 exceptAll 找出差异
        List<CSVRow> onlyInFile1 = exceptAll(rows1, rows2);
        List<CSVRow> onlyInFile2 = exceptAll(rows2, rows1);

        System.out.println("仅在文件1中: " + onlyInFile1.size() + " 行");
        System.out.println("仅在文件2中: " + onlyInFile2.size() + " 行");

        // 计算相同的行数
        long identicalRows = rows1.size() - onlyInFile1.size();

        // 获取列名
        List<String> columns = new ArrayList<>();
        if (!rows1.isEmpty()) {
            columns = new ArrayList<>(rows1.get(0).getData().keySet());
            Collections.sort(columns);
        }

        comparisonResult = new ComparisonResult(
                totalRows1, totalRows2, identicalRows,
                onlyInFile1, onlyInFile2, columns
        );

        return comparisonResult;
    }

    /**
     * 导出差异数据到CSV
     */
    public void exportDifferencesCSV(String outputPrefix) throws IOException {
        if (comparisonResult == null) {
            System.out.println("无对比结果可导出");
            return;
        }

        System.out.println("开始导出差异数据...");

        List<String> columns = comparisonResult.getColumns();

        // 导出仅在文件1中的数据
        if (!comparisonResult.getOnlyInFile1().isEmpty()) {
            String output1 = outputPrefix + "_only_in_file1.csv";
            exportToCSV(comparisonResult.getOnlyInFile1(), columns, output1);
            System.out.println("仅在文件1中的数据已导出: " + output1);
        }

        // 导出仅在文件2中的数据
        if (!comparisonResult.getOnlyInFile2().isEmpty()) {
            String output2 = outputPrefix + "_only_in_file2.csv";
            exportToCSV(comparisonResult.getOnlyInFile2(), columns, output2);
            System.out.println("仅在文件2中的数据已导出: " + output2);
        }
    }

    /**
     * 导出到CSV文件
     */
    private void exportToCSV(List<CSVRow> rows, List<String> columns, String filePath) 
            throws IOException {
        
        try (CSVWriter writer = new CSVWriter(new OutputStreamWriter(
                new FileOutputStream(filePath), StandardCharsets.UTF_8))) {

            // 写入表头
            writer.writeNext(columns.toArray(new String[0]));

            // 写入数据
            for (CSVRow row : rows) {
                String[] values = new String[columns.size()];
                for (int i = 0; i < columns.size(); i++) {
                    values[i] = row.getData().getOrDefault(columns.get(i), "");
                }
                writer.writeNext(values);
            }
        }
    }

    /**
     * 生成HTML报告
     */
    public void generateHTMLReport(String file1Path, String file2Path, String outputPath) 
            throws IOException {
        
        if (comparisonResult == null) {
            throw new IllegalStateException("必须先执行对比才能生成报告");
        }

        System.out.println("开始生成HTML报告...");

        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html>\n");
        html.append("<html>\n<head>\n");
        html.append("    <title>CSV对比报告 - Java版本</title>\n");
        html.append("    <meta charset=\"UTF-8\">\n");
        html.append("    <style>\n");
        html.append("        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }\n");
        html.append("        .container { max-width: 1400px; margin: 0 auto; background-color: white; padding: 30px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }\n");
        html.append("        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 30px; }\n");
        html.append("        .header h1 { margin: 0 0 15px 0; }\n");
        html.append("        .header p { margin: 5px 0; opacity: 0.9; }\n");
        html.append("        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }\n");
        html.append("        .stat-box { background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); color: white; padding: 25px; border-radius: 10px; text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }\n");
        html.append("        .stat-box h3 { margin: 0; font-size: 2.5em; font-weight: bold; }\n");
        html.append("        .stat-box p { margin: 10px 0 0 0; font-size: 1.1em; opacity: 0.9; }\n");
        html.append("        .section-title { color: #667eea; font-size: 1.5em; margin: 30px 0 15px 0; padding-bottom: 10px; border-bottom: 3px solid #667eea; }\n");
        html.append("        .diff-table { width: 100%; border-collapse: collapse; margin: 20px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }\n");
        html.append("        .diff-table th { background-color: #667eea; color: white; padding: 12px; text-align: left; font-weight: bold; }\n");
        html.append("        .diff-table td { border: 1px solid #ddd; padding: 10px; text-align: left; }\n");
        html.append("        .diff-table tr:nth-child(even) { background-color: #f9f9f9; }\n");
        html.append("        .diff-table tr:hover { background-color: #f0f0f0; }\n");
        html.append("        .no-data { color: #666; font-style: italic; text-align: center; padding: 30px; background-color: #f9f9f9; border-radius: 5px; }\n");
        html.append("        .badge { display: inline-block; padding: 5px 10px; background-color: #667eea; color: white; border-radius: 5px; font-size: 0.9em; margin-left: 10px; }\n");
        html.append("        .info-box { background-color: #e3f2fd; border-left: 4px solid #2196f3; padding: 15px; border-radius: 5px; margin: 20px 0; }\n");
        html.append("        .excluded-fields { background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; border-radius: 5px; margin: 15px 0; }\n");
        html.append("    </style>\n");
        html.append("</head>\n<body>\n");
        html.append("    <div class=\"container\">\n");
        html.append("        <div class=\"header\">\n");
        html.append("            <h1>🔍 CSV文件对比报告</h1>\n");
        html.append("            <p><strong>文件1:</strong> ").append(file1Path).append("</p>\n");
        html.append("            <p><strong>文件2:</strong> ").append(file2Path).append("</p>\n");
        html.append("            <p><strong>对比时间:</strong> ").append(new java.util.Date()).append("</p>\n");
        html.append("            <p><strong>对比引擎:</strong> Java (exceptAll实现) <span class=\"badge\">纯Java</span></p>\n");
        html.append("        </div>\n");

        if (!excludeFields.isEmpty()) {
            html.append("        <div class=\"excluded-fields\">\n");
            html.append("            <strong>⚠️ 排除比较的字段:</strong> ");
            html.append(String.join(", ", excludeFields));
            html.append("\n        </div>\n");
        }

        if (sampleRate < 1.0) {
            html.append("        <div class=\"info-box\">\n");
            html.append("            <strong>📊 抽样信息:</strong> 使用 ");
            html.append(String.format("%.1f", sampleRate * 100));
            html.append("% 抽样率进行对比\n");
            html.append("        </div>\n");
        }

        html.append("        <div class=\"summary\">\n");
        html.append("            <h2 class=\"section-title\">📈 对比摘要</h2>\n");
        html.append("            <div class=\"stats\">\n");
        html.append("                <div class=\"stat-box\">\n");
        html.append("                    <h3>").append(String.format("%,d", comparisonResult.getTotalRowsFile1())).append("</h3>\n");
        html.append("                    <p>文件1总行数</p>\n");
        html.append("                </div>\n");
        html.append("                <div class=\"stat-box\">\n");
        html.append("                    <h3>").append(String.format("%,d", comparisonResult.getTotalRowsFile2())).append("</h3>\n");
        html.append("                    <p>文件2总行数</p>\n");
        html.append("                </div>\n");
        html.append("                <div class=\"stat-box\">\n");
        html.append("                    <h3>").append(String.format("%,d", comparisonResult.getIdenticalRows())).append("</h3>\n");
        html.append("                    <p>完全相同行数</p>\n");
        html.append("                </div>\n");
        html.append("                <div class=\"stat-box\">\n");
        html.append("                    <h3>").append(String.format("%,d", comparisonResult.getOnlyInFile1().size())).append("</h3>\n");
        html.append("                    <p>仅在文件1中</p>\n");
        html.append("                </div>\n");
        html.append("                <div class=\"stat-box\">\n");
        html.append("                    <h3>").append(String.format("%,d", comparisonResult.getOnlyInFile2().size())).append("</h3>\n");
        html.append("                    <p>仅在文件2中</p>\n");
        html.append("                </div>\n");
        html.append("            </div>\n");
        html.append("        </div>\n");

        // 仅在文件1中的行
        html.append("        <div class=\"differences\">\n");
        html.append("            <h2 class=\"section-title\">📋 仅在文件1中的行 (前50条)</h2>\n");

        List<CSVRow> onlyInFile1 = comparisonResult.getOnlyInFile1();
        if (onlyInFile1.isEmpty()) {
            html.append("            <p class=\"no-data\">✓ 无独有数据</p>\n");
        } else {
            html.append("            <div style=\"overflow-x: auto;\"><table class=\"diff-table\">\n");
            html.append("                <tr>");
            for (String col : comparisonResult.getColumns()) {
                html.append("<th>").append(escapeHtml(col)).append("</th>");
            }
            html.append("</tr>\n");

            int limit = Math.min(50, onlyInFile1.size());
            for (int i = 0; i < limit; i++) {
                CSVRow row = onlyInFile1.get(i);
                html.append("                <tr>");
                for (String col : comparisonResult.getColumns()) {
                    String value = row.getData().getOrDefault(col, "");
                    html.append("<td>").append(escapeHtml(value)).append("</td>");
                }
                html.append("</tr>\n");
            }

            html.append("            </table></div>\n");
        }

        // 仅在文件2中的行
        html.append("            <h2 class=\"section-title\">📋 仅在文件2中的行 (前50条)</h2>\n");

        List<CSVRow> onlyInFile2 = comparisonResult.getOnlyInFile2();
        if (onlyInFile2.isEmpty()) {
            html.append("            <p class=\"no-data\">✓ 无独有数据</p>\n");
        } else {
            html.append("            <div style=\"overflow-x: auto;\"><table class=\"diff-table\">\n");
            html.append("                <tr>");
            for (String col : comparisonResult.getColumns()) {
                html.append("<th>").append(escapeHtml(col)).append("</th>");
            }
            html.append("</tr>\n");

            int limit = Math.min(50, onlyInFile2.size());
            for (int i = 0; i < limit; i++) {
                CSVRow row = onlyInFile2.get(i);
                html.append("                <tr>");
                for (String col : comparisonResult.getColumns()) {
                    String value = row.getData().getOrDefault(col, "");
                    html.append("<td>").append(escapeHtml(value)).append("</td>");
                }
                html.append("</tr>\n");
            }

            html.append("            </table></div>\n");
        }

        html.append("        </div>\n");

        html.append("        <div class=\"summary\">\n");
        html.append("            <h2 class=\"section-title\">ℹ️ 对比说明</h2>\n");
        html.append("            <div class=\"info-box\">\n");
        html.append("                <ul style=\"margin: 10px 0; padding-left: 20px;\">\n");
        html.append("                    <li><strong>对比方法:</strong> 纯Java实现的 <code>exceptAll()</code> 方法</li>\n");
        html.append("                    <li><strong>相同行:</strong> 两个文件中内容完全一致的行（包括重复行）</li>\n");
        html.append("                    <li><strong>仅在文件1中:</strong> 在文件1中存在但文件2中不存在的行</li>\n");
        html.append("                    <li><strong>仅在文件2中:</strong> 在文件2中存在但文件1中不存在的行</li>\n");
        html.append("                    <li><strong>重复处理:</strong> 如果某行在文件1中出现3次，在文件2中出现1次，则差异中会显示2次</li>\n");
        html.append("                </ul>\n");
        html.append("            </div>\n");
        html.append("        </div>\n");

        html.append("    </div>\n");
        html.append("</body>\n</html>");

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(outputPath), StandardCharsets.UTF_8))) {
            writer.write(html.toString());
        }

        System.out.println("HTML报告已生成: " + outputPath);
    }

    /**
     * HTML转义
     */
    private String escapeHtml(String input) {
        if (input == null) return "";
        return input.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }

    /**
     * 主函数
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            printUsage();
            System.exit(1);
        }

        String file1 = args[0];
        String file2 = args[1];

        // 解析命令行参数
        Set<String> excludeFields = new HashSet<>();
        double sampleRate = 1.0;
        String outputHtml = "comparison_report.html";
        String exportCsvPrefix = null;

        for (int i = 2; i < args.length; i++) {
            switch (args[i]) {
                case "--exclude":
                case "-e":
                    i++;
                    while (i < args.length && !args[i].startsWith("-")) {
                        excludeFields.add(args[i]);
                        i++;
                    }
                    i--;
                    break;
                case "--sample-rate":
                case "-s":
                    i++;
                    if (i < args.length) {
                        sampleRate = Double.parseDouble(args[i]);
                        if (sampleRate <= 0 || sampleRate > 1.0) {
                            System.err.println("错误: 抽样率必须在 0 和 1 之间");
                            System.exit(1);
                        }
                    }
                    break;
                case "--output":
                case "-o":
                    i++;
                    if (i < args.length) {
                        outputHtml = args[i];
                    }
                    break;
                case "--export-csv":
                    i++;
                    if (i < args.length) {
                        exportCsvPrefix = args[i];
                    }
                    break;
                case "--help":
                case "-h":
                    printUsage();
                    System.exit(0);
                    break;
            }
        }

        // 创建对比器并执行对比
        CSVComparator comparator = new CSVComparator(excludeFields, sampleRate);

        try {
            ComparisonResult result = comparator.compareCSVs(file1, file2);

            // 导出差异CSV
            if (exportCsvPrefix != null) {
                comparator.exportDifferencesCSV(exportCsvPrefix);
            }

            // 生成HTML报告
            comparator.generateHTMLReport(file1, file2, outputHtml);

            // 打印摘要
            System.out.println("\n" + "=".repeat(60));
            System.out.println("📊 对比完成");
            System.out.println("=".repeat(60));
            System.out.println(String.format("文件1总行数:    %,d", result.getTotalRowsFile1()));
            System.out.println(String.format("文件2总行数:    %,d", result.getTotalRowsFile2()));
            System.out.println(String.format("相同行数:      %,d", result.getIdenticalRows()));
            System.out.println(String.format("仅在文件1中:   %,d 行", result.getOnlyInFile1().size()));
            System.out.println(String.format("仅在文件2中:   %,d 行", result.getOnlyInFile2().size()));
            System.out.println("HTML报告:      " + outputHtml);

            if (exportCsvPrefix != null) {
                System.out.println("差异数据已导出到: " + exportCsvPrefix + "_only_in_file*.csv");
            }

            System.out.println("=".repeat(60));

        } catch (Exception e) {
            System.err.println("对比过程出错: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * 打印使用说明
     */
    private static void printUsage() {
        System.out.println("CSV自动对比工具 - Java版本 (exceptAll实现)");
        System.out.println();
        System.out.println("使用方法:");
        System.out.println("  java CSVComparator <file1> <file2> [options]");
        System.out.println();
        System.out.println("参数:");
        System.out.println("  file1                    第一个CSV文件路径");
        System.out.println("  file2                    第二个CSV文件路径");
        System.out.println();
        System.out.println("选项:");
        System.out.println("  --exclude, -e <fields>   排除不比较的字段列表");
        System.out.println("  --sample-rate, -s <rate> 抽样率 (0-1)，默认1.0


```java
        System.out.println("  --output, -o <file>      输出HTML报告路径，默认comparison_report.html");
        System.out.println("  --export-csv <prefix>    导出差异数据CSV路径前缀");
        System.out.println("  --help, -h               显示帮助信息");
        System.out.println();
        System.out.println("示例:");
        System.out.println("  # 基本对比");
        System.out.println("  java CSVComparator file1.csv file2.csv");
        System.out.println();
        System.out.println("  # 排除某些字段");
        System.out.println("  java CSVComparator file1.csv file2.csv --exclude timestamp update_time");
        System.out.println();
        System.out.println("  # 10%抽样对比");
        System.out.println("  java CSVComparator file1.csv file2.csv --sample-rate 0.1");
        System.out.println();
        System.out.println("  # 导出差异数据");
        System.out.println("  java CSVComparator file1.csv file2.csv --export-csv diff_result");
        System.out.println();
        System.out.println("  # 完整示例");
        System.out.println("  java CSVComparator file1.csv file2.csv \\");
        System.out.println("       --exclude id timestamp \\");
        System.out.println("       --sample-rate 0.5 \\");
        System.out.println("       --output report.html \\");
        System.out.println("       --export-csv differences");
    }
}
```

现在创建 `pom.xml` 文件用于Maven项目管理：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.csvtools</groupId>
    <artifactId>csv-comparator</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <name>CSV Comparator</name>
    <description>CSV自动对比工具 - 实现类似Spark exceptAll的功能</description>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.source>11</maven.compiler.source>
        <maven.compiler.target>11</maven.compiler.target>
    </properties>

    <dependencies>
        <!-- OpenCSV for CSV parsing -->
        <dependency>
            <groupId>com.opencsv</groupId>
            <artifactId>opencsv</artifactId>
            <version>5.7.1</version>
        </dependency>

        <!-- Apache Commons Lang for utilities -->
        <dependency>
            <groupId>org.apache.commons</groupId>
            <artifactId>commons-lang3</artifactId>
            <version>3.12.0</version>
        </dependency>

        <!-- JUnit for testing (optional) -->
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.13.2</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <!-- Maven Compiler Plugin -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.10.1</version>
                <configuration>
                    <source>11</source>
                    <target>11</target>
                </configuration>
            </plugin>

            <!-- Maven Assembly Plugin - 创建可执行JAR -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>3.4.2</version>
                <configuration>
                    <archive>
                        <manifest>
                            <mainClass>CSVComparator</mainClass>
                        </manifest>
                    </archive>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                </configuration>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>

            <!-- Maven Shade Plugin - 替代方案 -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.4.1</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>CSVComparator</mainClass>
                                </transformer>
                            </transformers>
                            <finalName>csv-comparator</finalName>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

创建 `build.gradle` 文件用于Gradle项目管理：

```gradle
plugins {
    id 'java'
    id 'application'
    id 'com.github.johnrengelman.shadow' version '7.1.2'
}

group = 'com.csvtools'
version = '1.0.0'
sourceCompatibility = '11'

repositories {
    mavenCentral()
}

dependencies {
    // OpenCSV for CSV parsing
    implementation 'com.opencsv:opencsv:5.7.1'

    // Apache Commons Lang
    implementation 'org.apache.commons:commons-lang3:3.12.0'

    // JUnit for testing
    testImplementation 'junit:junit:4.13.2'
}

application {
    mainClass = 'CSVComparator'
}

// 创建可执行JAR
jar {
    manifest {
        attributes 'Main-Class': 'CSVComparator'
    }
}

// Shadow JAR配置
shadowJar {
    archiveBaseName.set('csv-comparator')
    archiveClassifier.set('')
    archiveVersion.set('1.0.0')
}
```

创建示例测试文件 `CSVComparatorTest.java`：

```java
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class CSVComparatorTest {

    private File tempDir;

    @Before
    public void setUp() {
        tempDir = new File(System.getProperty("java.io.tmpdir"), "csv-test-" + System.currentTimeMillis());
        tempDir.mkdirs();
    }

    @Test
    public void testExceptAllBasic() {
        CSVComparator comparator = new CSVComparator(new HashSet<>(), 1.0);

        // 创建测试数据
        List<CSVComparator.CSVRow> list1 = new ArrayList<>();
        List<CSVComparator.CSVRow> list2 = new ArrayList<>();

        Map<String, String> data1 = new HashMap<>();
        data1.put("col1", "value1");
        data1.put("col2", "value2");

        Map<String, String> data2 = new HashMap<>();
        data2.put("col1", "value3");
        data2.put("col2", "value4");

        list1.add(new CSVComparator.CSVRow(data1, "hash1", 0));
        list1.add(new CSVComparator.CSVRow(data2, "hash2", 1));

        list2.add(new CSVComparator.CSVRow(data2, "hash2", 0));

        // 执行exceptAll
        List<CSVComparator.CSVRow> result = comparator.exceptAll(list1, list2);

        // 验证结果
        assertEquals(1, result.size());
        assertEquals("hash1", result.get(0).getHash());
    }

    @Test
    public void testExceptAllWithDuplicates() {
        CSVComparator comparator = new CSVComparator(new HashSet<>(), 1.0);

        List<CSVComparator.CSVRow> list1 = new ArrayList<>();
        List<CSVComparator.CSVRow> list2 = new ArrayList<>();

        Map<String, String> data = new HashMap<>();
        data.put("col1", "value1");

        // list1中有3个相同的行
        list1.add(new CSVComparator.CSVRow(data, "hash1", 0));
        list1.add(new CSVComparator.CSVRow(data, "hash1", 1));
        list1.add(new CSVComparator.CSVRow(data, "hash1", 2));

        // list2中有1个相同的行
        list2.add(new CSVComparator.CSVRow(data, "hash1", 0));

        // 执行exceptAll
        List<CSVComparator.CSVRow> result = comparator.exceptAll(list1, list2);

        // 应该返回2个差异行 (3 - 1 = 2)
        assertEquals(2, result.size());
    }

    @Test
    public void testExceptAllEmpty() {
        CSVComparator comparator = new CSVComparator(new HashSet<>(), 1.0);

        List<CSVComparator.CSVRow> list1 = new ArrayList<>();
        List<CSVComparator.CSVRow> list2 = new ArrayList<>();

        List<CSVComparator.CSVRow> result = comparator.exceptAll(list1, list2);

        assertEquals(0, result.size());
    }

    @Test
    public void testCompareCSVsSimple() throws Exception {
        // 创建测试CSV文件
        File csv1 = new File(tempDir, "test1.csv");
        File csv2 = new File(tempDir, "test2.csv");

        try (FileWriter writer1 = new FileWriter(csv1)) {
            writer1.write("id,name,age\n");
            writer1.write("1,Alice,30\n");
            writer1.write("2,Bob,25\n");
            writer1.write("3,Charlie,35\n");
        }

        try (FileWriter writer2 = new FileWriter(csv2)) {
            writer2.write("id,name,age\n");
            writer2.write("2,Bob,25\n");
            writer2.write("3,Charlie,35\n");
            writer2.write("4,David,40\n");
        }

        CSVComparator comparator = new CSVComparator(new HashSet<>(), 1.0);
        CSVComparator.ComparisonResult result = comparator.compareCSVs(
                csv1.getAbsolutePath(),
                csv2.getAbsolutePath()
        );

        // 验证结果
        assertEquals(3, result.getTotalRowsFile1());
        assertEquals(3, result.getTotalRowsFile2());
        assertEquals(2, result.getIdenticalRows());
        assertEquals(1, result.getOnlyInFile1().size());
        assertEquals(1, result.getOnlyInFile2().size());

        // 验证具体数据
        CSVComparator.CSVRow onlyInFile1 = result.getOnlyInFile1().get(0);
        assertEquals("Alice", onlyInFile1.getData().get("name"));

        CSVComparator.CSVRow onlyInFile2 = result.getOnlyInFile2().get(0);
        assertEquals("David", onlyInFile2.getData().get("name"));
    }

    @Test
    public void testExcludeFields() throws Exception {
        File csv1 = new File(tempDir, "test1.csv");
        File csv2 = new File(tempDir, "test2.csv");

        try (FileWriter writer1 = new FileWriter(csv1)) {
            writer1.write("id,name,timestamp\n");
            writer1.write("1,Alice,2024-01-01\n");
            writer1.write("2,Bob,2024-01-02\n");
        }

        try (FileWriter writer2 = new FileWriter(csv2)) {
            writer2.write("id,name,timestamp\n");
            writer2.write("1,Alice,2024-01-10\n");
            writer2.write("2,Bob,2024-01-11\n");
        }

        // 排除timestamp字段
        Set<String> excludeFields = new HashSet<>();
        excludeFields.add("timestamp");

        CSVComparator comparator = new CSVComparator(excludeFields, 1.0);
        CSVComparator.ComparisonResult result = comparator.compareCSVs(
                csv1.getAbsolutePath(),
                csv2.getAbsolutePath()
        );

        // 排除timestamp后，两个文件应该完全相同
        assertEquals(2, result.getIdenticalRows());
        assertEquals(0, result.getOnlyInFile1().size());
        assertEquals(0, result.getOnlyInFile2().size());
    }
}
```

创建 `README.md` 文档：

```markdown
# CSV Comparator - Java版本

纯Java实现的CSV文件对比工具，实现了类似Spark `exceptAll()` 的功能，无需安装Spark环境。

## 特性

- ✅ **纯Java实现**：无需Spark环境，轻量级
- ✅ **exceptAll实现**：完全模拟Spark的exceptAll行为，保留重复行
- ✅ **排除字段**：可以排除不需要比较的字段（如时间戳）
- ✅ **抽样支持**：支持按比例抽样大文件
- ✅ **HTML报告**：生成美观的可视化对比报告
- ✅ **导出差异**：将差异数据导出为CSV文件
- ✅ **高性能**：使用哈希算法优化对比性能

## 安装依赖

### 使用Maven

```bash
mvn clean package
```

### 使用Gradle

```bash
gradle shadowJar
```

### 手动编译

```bash
# 下载依赖
wget https://repo1.maven.org/maven2/com/opencsv/opencsv/5.7.1/opencsv-5.7.1.jar
wget https://repo1.maven.org/maven2/org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0.jar
wget https://repo1.maven.org/maven2/commons-beanutils/commons-beanutils/1.9.4/commons-beanutils-1.9.4.jar
wget https://repo1.maven.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar
wget https://repo1.maven.org/maven2/org/apache/commons/commons-text/1.10.0/commons-text-1.10.0.jar
wget https://repo1.maven.org/maven2/org/apache/commons/commons-collections4/4.4/commons-collections4-4.4.jar

# 编译
javac -cp "opencsv-5.7.1.jar:commons-lang3-3.12.0.jar:commons-beanutils-1.9.4.jar:commons-logging-1.2.jar:commons-text-1.10.0.jar:commons-collections4-4.4.jar" CSVComparator.java

# 运行
java -cp ".:opencsv-5.7.1.jar:commons-lang3-3.12.0.jar:commons-beanutils-1.9.4.jar:commons-logging-1.2.jar:commons-text-1.10.0.jar:commons-collections4-4.4.jar" CSVComparator file1.csv file2.csv
```

## 使用方法

### 基本用法

```bash
java -jar csv-comparator.jar file1.csv file2.csv
```

### 排除某些字段

```bash
java -jar csv-comparator.jar file1.csv file2.csv --exclude timestamp update_time created_at
```

### 抽样对比（适合大文件）

```bash
# 10%抽样
java -jar csv-comparator.jar file1.csv file2.csv --sample-rate 0.1

# 50%抽样
java -jar csv-comparator.jar file1.csv file2.csv --sample-rate 0.5
```

### 自定义输出路径

```bash
java -jar csv-comparator.jar file1.csv file2.csv --output my_report.html
```

### 导出差异数据

```bash
java -jar csv-comparator.jar file1.csv file2.csv --export-csv differences

# 会生成两个文件:
# differences_only_in_file1.csv
# differences_only_in_file2.csv
```

### 完整示例

```bash
java -jar csv-comparator.jar \
    data/orders_2023.csv \
    data/orders_2024.csv \
    --exclude order_id timestamp \
    --sample-rate 0.2 \
    --output reports/orders_comparison.html \
    --export-csv reports/orders_diff
```

## exceptAll 实现原理

本工具实现了与Spark完全相同的 `exceptAll()` 语义：

```java
// 伪代码说明
df1.exceptAll(df2) 的含义:
- 返回在df1中但不在df2中的所有行
- 保留重复行的差异

示例:
df1: [A, A, A, B, C]
df2: [A, B, B]

df1.exceptAll(df2) = [A, A, C]
解释:
- A在df1中出现3次，在df2中出现1次，差异为2次
- B在df1中出现1次，在df2中出现2次，差异为0次
- C在df1中出现1次，在df2中出现0次，差异为1次
```

核心算法：

```java
public List<CSVRow> exceptAll(List<CSVRow> list1, List<CSVRow> list2) {
    // 1. 统计list2中每个哈希的出现次数
    Map<String, Integer> list2Counts = countOccurrences(list2);

    // 2. 统计list1中每个哈希的出现次数
    Map<String, Integer> list1Counts = countOccurrences(list1);

    // 3. 计算差异: count(list1) - count(list2)
    List<CSVRow> result = new ArrayList<>();
    for (CSVRow row : list1) {
        int countInList1 = list1Counts.get(row.getHash());
        int countInList2 = list2Counts.getOrDefault(row.getHash(), 0);

        if (countInList1 > countInList2) {
            result.add(row);
            // 只添加差异数量的行
        }
    }

    return result;
}
```

## 性能优化

1. **MD5哈希**：对每行内容计算MD5哈希，O(1)时间复杂度比较
2. **HashMap计数**：使用HashMap统计行出现次数，高效
3. **内存管理**：适合中等规模文件（百万行级别）
4. **抽样支持**：大文件可使用抽样减少内存使用

## 输出说明

### HTML报告包含：

- 📊 **对比摘要**：总行数、相同行数、差异行数统计
- 📋 **差异数据**：展示前50条差异记录
- 💡 **可视化设计**：美观的渐变色卡片和表格
- ℹ️ **对比说明**：详细的对比方法说明

### CSV导出包含：

- `*_only_in_file1.csv`：仅在第一个文件中的行
- `*_only_in_file2.csv`：仅在第二个文件中的行

## 测试

```bash
# Maven
mvn test

# Gradle
gradle test

# 手动运行测试
javac -cp ".:junit-4.13.2.jar:..." CSVComparatorTest.java
java -cp ".:junit-4.13.2.jar:..." org.junit.runner.JUnitCore CSVComparatorTest
```

## 与Spark版本对比

| 特性 | Java版本 | Spark版本 |
|-----|---------|-----------|
| 安装复杂度 | ⭐ 简单 | ⭐⭐⭐ 复杂 |
| 运行环境 | JRE 11+ | Spark环境 |
| 内存使用 | 中等 | 较大 |
| 分布式 | ❌ | ✅ |
| 适用场景 | 中小文件 | 大规模数据 |
| 启动速度 | 快 | 较慢 |

## 限制

- 文件需要UTF-8编码
- 两个CSV必须有相同的列（顺序可以不同）
- 内存限制：建议单文件不超过1GB（可通过抽样处理更大文件）
- 不支持分布式计算

## 故障排除

### OutOfMemoryError

```bash
# 增加JVM堆内存
java -Xmx4g -jar csv-comparator.jar file1.csv file2.csv
```

### 字符编码问题

确保CSV文件是UTF-8编码：

```bash
# Linux/Mac转换编码
iconv -f GBK -t UTF-8 input.csv > output.csv
```

## 许可证

MIT License

## 贡献

欢迎提交Issue和Pull Request！
```

创建Windows批处理脚本 `compare.bat`：

```batch
@echo off
setlocal

set JAR_FILE=csv-comparator.jar
set JAVA_OPTS=-Xmx2g -Dfile.encoding=UTF-8

if not exist %JAR_FILE% (
    echo Error: %JAR_FILE% not found!
    echo Please build the project first: mvn clean package
    exit /b 1
)

java %JAVA_OPTS% -jar %JAR_FILE% %*
```

创建Linux/Mac Shell脚本 `compare.sh`：

```bash
#!/bin/bash

JAR_FILE="csv-comparator.jar"
JAVA_OPTS="-Xmx2g -Dfile.encoding=UTF-8"

if [ ! -f "$JAR_FILE" ]; then
    echo "Error: $JAR_FILE not found!"
    echo "Please build the project first: mvn clean package"
    exit 1
fi

java $JAVA_OPTS -jar $JAR_FILE "$@"
```

使脚本可执行：

```bash
chmod +x compare.sh
```

## 使用示例

编译并运行完整示例：

```bash
# 使用Maven编译
mvn clean package

# 运行示例
java -jar target/csv-comparator.jar test1.csv test2.csv

# 或使用脚本
./compare.sh test1.csv test2.csv --exclude timestamp --sample-rate 0.5 --output report.html --export-csv diff
```

这个Java版本完全实现了Spark的`exceptAll()`语义，无需安装Spark环境，适合中小规模的CSV对比任务！

