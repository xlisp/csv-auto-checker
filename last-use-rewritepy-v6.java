import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * 高性能CSV对比工具 - Java 8版本
 */
public class CSVComparator {
    
    private final List<String> primaryKeys;
    private final double sampleRate;
    private final Set<String> excludeFields;
    private ComparisonResult comparisonResults;
    
    public CSVComparator(List<String> primaryKeys, double sampleRate, List<String> excludeFields) {
        this.primaryKeys = primaryKeys;
        this.sampleRate = sampleRate;
        this.excludeFields = excludeFields != null ? new HashSet<>(excludeFields) : new HashSet<>();
    }
    
    /**
     * 分块加载大CSV文件
     */
    public List<Map<String, String>> loadCSV(String filePath) throws IOException {
        log("开始加载CSV文件: " + filePath);
        List<Map<String, String>> data = new ArrayList<>();
        
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8))) {
            
            String headerLine = br.readLine();
            if (headerLine == null) {
                throw new IOException("CSV文件为空");
            }
            
            String[] headers = parseCsvLine(headerLine);
            String line;
            
            while ((line = br.readLine()) != null) {
                String[] values = parseCsvLine(line);
                Map<String, String> row = new LinkedHashMap<>();
                
                for (int i = 0; i < headers.length && i < values.length; i++) {
                    row.put(headers[i].trim(), values[i].trim());
                }
                data.add(row);
            }
        }
        
        log("成功加载 " + data.size() + " 行数据");
        return data;
    }
    
    /**
     * 解析CSV行（处理引号和逗号）
     */
    private String[] parseCsvLine(String line) {
        List<String> result = new ArrayList<>();
        boolean inQuotes = false;
        StringBuilder current = new StringBuilder();
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                result.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        result.add(current.toString());
        
        return result.toArray(new String[0]);
    }
    
    /**
     * 创建组合主键
     */
    private String createCompositeKey(Map<String, String> row) {
        if (primaryKeys.size() == 1) {
            return normalizeValue(row.get(primaryKeys.get(0)));
        }
        
        return primaryKeys.stream()
                .map(key -> normalizeValue(row.get(key)))
                .collect(Collectors.joining("|"));
    }
    
    /**
     * 标准化数值（处理科学计数法，确保正负数一致）
     */
    private String normalizeValue(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        
        // 尝试解析为数值
        try {
            // 去除前后空格
            value = value.trim();
            
            // 检查是否为科学计数法
            if (value.matches("^[+-]?\\d+\\.?\\d*[eE][+-]?\\d+$") || 
                value.matches("^[+-]?\\d+\\.\\d+$") ||
                value.matches("^[+-]?\\d+$")) {
                
                BigDecimal bd = new BigDecimal(value);
                // 转换为普通表示法，去除尾部零
                return bd.stripTrailingZeros().toPlainString();
            }
        } catch (NumberFormatException e) {
            // 不是数值，直接返回原值
        }
        
        return value;
    }
    
    /**
     * 智能抽样策略
     */
    public SampledData intelligentSampling(List<Map<String, String>> data1, 
                                          List<Map<String, String>> data2) {
        
        // 创建主键映射
        Map<String, Map<String, String>> map1 = data1.stream()
                .collect(Collectors.toMap(this::createCompositeKey, row -> row, (a, b) -> a));
        
        Map<String, Map<String, String>> map2 = data2.stream()
                .collect(Collectors.toMap(this::createCompositeKey, row -> row, (a, b) -> a));
        
        // 找到共同的主键
        Set<String> commonKeys = new HashSet<>(map1.keySet());
        commonKeys.retainAll(map2.keySet());
        
        log("找到 " + commonKeys.size() + " 个共同主键");
        
        if (commonKeys.isEmpty()) {
            log("警告: 未找到共同的主键");
            return new SampledData(new ArrayList<>(), new ArrayList<>(), new HashSet<>());
        }
        
        // 抽样
        Set<String> sampledKeys;
        if (sampleRate >= 1.0 || commonKeys.size() <= 100) {
            sampledKeys = commonKeys;
        } else {
            int sampleSize = (int) (commonKeys.size() * sampleRate);
            List<String> keyList = new ArrayList<>(commonKeys);
            Collections.shuffle(keyList);
            sampledKeys = new HashSet<>(keyList.subList(0, sampleSize));
        }
        
        // 筛选数据
        List<Map<String, String>> sampled1 = sampledKeys.stream()
                .map(map1::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        
        List<Map<String, String>> sampled2 = sampledKeys.stream()
                .map(map2::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        
        log("智能抽样完成: " + sampledKeys.size() + " 个主键");
        
        return new SampledData(sampled1, sampled2, sampledKeys);
    }
    
    /**
     * 并行对比数据
     */
    public ComparisonResult parallelCompare(List<Map<String, String>> data1,
                                           List<Map<String, String>> data2) 
            throws InterruptedException, ExecutionException {
        
        // 创建索引
        Map<String, Map<String, String>> indexed1 = data1.stream()
                .collect(Collectors.toMap(this::createCompositeKey, row -> row, (a, b) -> a));
        
        Map<String, Map<String, String>> indexed2 = data2.stream()
                .collect(Collectors.toMap(this::createCompositeKey, row -> row, (a, b) -> a));
        
        // 计算共同主键
        Set<String> allKeys = new HashSet<>(indexed1.keySet());
        allKeys.addAll(indexed2.keySet());
        
        Set<String> commonKeys = new HashSet<>(indexed1.keySet());
        commonKeys.retainAll(indexed2.keySet());
        
        Set<String> onlyInData1 = new HashSet<>(indexed1.keySet());
        onlyInData1.removeAll(indexed2.keySet());
        
        Set<String> onlyInData2 = new HashSet<>(indexed2.keySet());
        onlyInData2.removeAll(indexed1.keySet());
        
        ComparisonResult result = new ComparisonResult();
        result.totalKeys = allKeys.size();
        result.commonKeys = commonKeys.size();
        result.onlyInData1 = onlyInData1.size();
        result.onlyInData2 = onlyInData2.size();
        
        // 并行处理
        int chunkSize = 1000;
        List<String> commonKeyList = new ArrayList<>(commonKeys);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Future<ChunkResult>> futures = new ArrayList<>();
        
        for (int i = 0; i < commonKeyList.size(); i += chunkSize) {
            int end = Math.min(i + chunkSize, commonKeyList.size());
            List<String> chunk = commonKeyList.subList(i, end);
            
            futures.add(executor.submit(() -> compareChunk(indexed1, indexed2, chunk)));
        }
        
        // 收集结果
        for (Future<ChunkResult> future : futures) {
            ChunkResult chunkResult = future.get();
            result.differences.addAll(chunkResult.differences);
            result.identicalRows += chunkResult.identicalRows;
        }
        
        executor.shutdown();
        
        log("对比完成: " + result.commonKeys + " 个共同主键");
        return result;
    }
    
    /**
     * 对比数据块
     */
    private ChunkResult compareChunk(Map<String, Map<String, String>> indexed1,
                                    Map<String, Map<String, String>> indexed2,
                                    List<String> keys) {
        
        ChunkResult result = new ChunkResult();
        
        for (String key : keys) {
            Map<String, String> row1 = indexed1.get(key);
            Map<String, String> row2 = indexed2.get(key);
            
            if (row1 == null || row2 == null) {
                continue;
            }
            
            List<FieldDifference> fieldDiffs = new ArrayList<>();
            
            // 对比每个字段
            Set<String> allFields = new HashSet<>(row1.keySet());
            allFields.addAll(row2.keySet());
            
            for (String field : allFields) {
                if (excludeFields.contains(field) || primaryKeys.contains(field)) {
                    continue;
                }
                
                String val1 = normalizeValue(row1.getOrDefault(field, ""));
                String val2 = normalizeValue(row2.getOrDefault(field, ""));
                
                if (!val1.equals(val2)) {
                    fieldDiffs.add(new FieldDifference(field, 
                            row1.getOrDefault(field, ""), 
                            row2.getOrDefault(field, "")));
                }
            }
            
            if (!fieldDiffs.isEmpty()) {
                result.differences.add(new RowDifference(key, fieldDiffs));
            } else {
                result.identicalRows++;
            }
        }
        
        return result;
    }
    
    /**
     * 对比CSV文件
     */
    public ComparisonResult compareCSVs(String file1, String file2, String outputHtml) 
            throws IOException, InterruptedException, ExecutionException {
        
        log("开始CSV对比流程");
        
        // 加载数据
        List<Map<String, String>> data1 = loadCSV(file1);
        List<Map<String, String>> data2 = loadCSV(file2);
        
        // 验证主键
        for (String key : primaryKeys) {
            if (!data1.get(0).containsKey(key)) {
                throw new IllegalArgumentException("主键 '" + key + "' 不存在于文件1中");
            }
            if (!data2.get(0).containsKey(key)) {
                throw new IllegalArgumentException("主键 '" + key + "' 不存在于文件2中");
            }
        }
        
        if (!excludeFields.isEmpty()) {
            log("排除比较的字段: " + String.join(", ", excludeFields));
        }
        
        // 智能抽样
        SampledData sampled = intelligentSampling(data1, data2);
        
        // 执行对比
        ComparisonResult result = parallelCompare(sampled.data1, sampled.data2);
        
        // 生成HTML报告
        generateHTMLReport(result, file1, file2, outputHtml);
        
        this.comparisonResults = result;
        return result;
    }
    
    /**
     * 生成HTML报告
     */
    private void generateHTMLReport(ComparisonResult result, String file1, 
                                   String file2, String outputPath) throws IOException {
        
        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html>\n<html>\n<head>\n");
        html.append("    <title>CSV对比报告</title>\n");
        html.append("    <meta charset=\"UTF-8\">\n");
        html.append("    <style>\n");
        html.append("        body { font-family: Arial, sans-serif; margin: 20px; }\n");
        html.append("        .header { background-color: #f0f0f0; padding: 20px; border-radius: 5px; }\n");
        html.append("        .stats { display: flex; justify-content: space-around; margin: 20px 0; flex-wrap: wrap; }\n");
        html.append("        .stat-box { background-color: #e8f4f8; padding: 15px; border-radius: 5px; text-align: center; margin: 10px; min-width: 150px; }\n");
        html.append("        .diff-row { border: 1px solid #ddd; margin: 10px 0; padding: 10px; border-radius: 5px; }\n");
        html.append("        .field-diff { margin: 5px 0; padding: 5px; background-color: #fff3cd; border-radius: 3px; }\n");
        html.append("        .key { font-weight: bold; color: #0066cc; }\n");
        html.append("        .value1 { color: #d32f2f; }\n");
        html.append("        .value2 { color: #388e3c; }\n");
        html.append("    </style>\n");
        html.append("</head>\n<body>\n");
        
        // Header
        html.append("    <div class=\"header\">\n");
        html.append("        <h1>CSV文件对比报告</h1>\n");
        html.append("        <p><strong>文件1:</strong> ").append(file1).append("</p>\n");
        html.append("        <p><strong>文件2:</strong> ").append(file2).append("</p>\n");
        html.append("        <p><strong>对比时间:</strong> ").append(
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("</p>\n");
        html.append("        <p><strong>主键:</strong> ").append(String.join(", ", primaryKeys)).append("</p>\n");
        
        if (!excludeFields.isEmpty()) {
            html.append("        <p><strong>排除的字段:</strong> ").append(
                    String.join(", ", excludeFields)).append("</p>\n");
        }
        
        html.append("    </div>\n");
        
        // Stats
        html.append("    <div class=\"summary\">\n");
        html.append("        <h2>对比摘要</h2>\n");
        html.append("        <div class=\"stats\">\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.totalKeys)
            .append("</h3><p>总主键数</p></div>\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.commonKeys)
            .append("</h3><p>共同主键数</p></div>\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.onlyInData1)
            .append("</h3><p>仅在文件1中</p></div>\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.onlyInData2)
            .append("</h3><p>仅在文件2中</p></div>\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.identicalRows)
            .append("</h3><p>完全相同行数</p></div>\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.differences.size())
            .append("</h3><p>存在差异行数</p></div>\n");
        html.append("        </div>\n");
        html.append("    </div>\n");
        
        // Differences
        html.append("    <div class=\"differences\">\n");
        html.append("        <h2>详细差异 (显示前100条)</h2>\n");
        
        if (result.differences.isEmpty()) {
            html.append("        <p>未发现数据差异</p>\n");
        } else {
            int limit = Math.min(100, result.differences.size());
            for (int i = 0; i < limit; i++) {
                RowDifference diff = result.differences.get(i);
                html.append("        <div class=\"diff-row\">\n");
                html.append("            <p class=\"key\">主键: ").append(escapeHtml(diff.key)).append("</p>\n");
                
                for (FieldDifference fd : diff.differences) {
                    html.append("            <div class=\"field-diff\">\n");
                    html.append("                <strong>").append(escapeHtml(fd.field)).append(":</strong> ");
                    html.append("                <span class=\"value1\">文件1: ").append(escapeHtml(fd.value1)).append("</span> → ");
                    html.append("                <span class=\"value2\">文件2: ").append(escapeHtml(fd.value2)).append("</span>\n");
                    html.append("            </div>\n");
                }
                
                html.append("        </div>\n");
            }
        }
        
        html.append("    </div>\n");
        html.append("</body>\n</html>");
        
        Files.write(Paths.get(outputPath), html.toString().getBytes(StandardCharsets.UTF_8));
        log("HTML报告已生成: " + outputPath);
    }
    
    private String escapeHtml(String text) {
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;");
    }
    
    private void log(String message) {
        System.out.println("[" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "] " + message);
    }
    
    // 内部类
    static class SampledData {
        List<Map<String, String>> data1;
        List<Map<String, String>> data2;
        Set<String> sampledKeys;
        
        SampledData(List<Map<String, String>> data1, List<Map<String, String>> data2, Set<String> sampledKeys) {
            this.data1 = data1;
            this.data2 = data2;
            this.sampledKeys = sampledKeys;
        }
    }
    
    static class ComparisonResult {
        int totalKeys;
        int commonKeys;
        int onlyInData1;
        int onlyInData2;
        int identicalRows;
        List<RowDifference> differences = new ArrayList<>();
    }
    
    static class RowDifference {
        String key;
        List<FieldDifference> differences;
        
        RowDifference(String key, List<FieldDifference> differences) {
            this.key = key;
            this.differences = differences;
        }
    }
    
    static class FieldDifference {
        String field;
        String value1;
        String value2;
        
        FieldDifference(String field, String value1, String value2) {
            this.field = field;
            this.value1 = value1;
            this.value2 = value2;
        }
    }
    
    static class ChunkResult {
        List<RowDifference> differences = new ArrayList<>();
        int identicalRows = 0;
    }
    
    /**
     * 主函数
     */
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("用法: java CSVComparator <file1> <file2> <key1[,key2,...]> [options]");
            System.out.println("选项:");
            System.out.println("  --sample-rate=<rate>  抽样率 (0-1), 默认0.1");
            System.out.println("  --exclude=<field1,field2,...>  排除的字段");
            System.out.println("  --output=<path>  输出HTML路径, 默认comparison_report.html");
            return;
        }
        
        String file1 = args[0];
        String file2 = args[1];
        List<String> keys = Arrays.asList(args[2].split(","));
        
        double sampleRate = 0.1;
        List<String> excludeFields = new ArrayList<>();
        String output = "comparison_report.html";
        
        // 解析参数
        for (int i = 3; i < args.length; i++) {
            if (args[i].startsWith("--sample-rate=")) {
                sampleRate = Double.parseDouble(args[i].substring(14));
            } else if (args[i].startsWith("--exclude=")) {
                excludeFields = Arrays.asList(args[i].substring(10).split(","));
            } else if (args[i].startsWith("--output=")) {
                output = args[i].substring(9);
            }
        }
        
        try {
            CSVComparator comparator = new CSVComparator(keys, sampleRate, excludeFields);
            ComparisonResult result = comparator.compareCSVs(file1, file2, output);
            
            System.out.println("\n=== 对比完成 ===");
            System.out.println("总主键数: " + result.totalKeys);
            System.out.println("共同主键数: " + result.commonKeys);
            System.out.println("相同行数: " + result.identicalRows);
            System.out.println("差异行数: " + result.differences.size());
            System.out.println("HTML报告: " + output);
            
        } catch (Exception e) {
            System.err.println("对比过程出错: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
