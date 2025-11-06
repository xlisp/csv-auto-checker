import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.logging.*;
import java.util.regex.Pattern;

/**
 * CSV自动对比工具 - Java版本（按字段组分类）
 * 基于主键的CSV对比，支持科学表示法，按不同字段组合分类差异
 */
public class CSVComparator {
    
    private static final Logger logger = Logger.getLogger(CSVComparator.class.getName());
    private static final Pattern SCIENTIFIC_NOTATION = Pattern.compile("^[+-]?\\d+\\.?\\d*[eE][+-]?\\d+$");
    private static final Pattern DECIMAL_NUMBER = Pattern.compile("^[+-]?\\d+\\.?\\d*$");
    
    private final List<String> primaryKeys;
    private final double sampleRate;
    private final Set<String> excludeFields;
    private final double numericTolerance;
    private final boolean enableNumericComparison;
    private ComparisonResult comparisonResults;
    
    public CSVComparator(List<String> primaryKeys, double sampleRate, List<String> excludeFields, 
                        double numericTolerance, boolean enableNumericComparison) {
        this.primaryKeys = primaryKeys;
        this.sampleRate = sampleRate;
        this.excludeFields = excludeFields != null ? new HashSet<>(excludeFields) : new HashSet<>();
        this.numericTolerance = numericTolerance;
        this.enableNumericComparison = enableNumericComparison;
        configureLogger();
    }
    
    private void configureLogger() {
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        logger.addHandler(handler);
        logger.setLevel(Level.INFO);
    }
    
    private boolean isNumeric(String str) {
        if (str == null || str.trim().isEmpty()) {
            return false;
        }
        String trimmed = str.trim();
        return SCIENTIFIC_NOTATION.matcher(trimmed).matches() || 
               DECIMAL_NUMBER.matcher(trimmed).matches();
    }
    
    private Double parseNumeric(String str) {
        try {
            return Double.parseDouble(str.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    private boolean valuesEqual(String val1, String val2) {
        if (val1 == null) val1 = "";
        if (val2 == null) val2 = "";
        
        if (val1.equals(val2)) {
            return true;
        }
        
        if (enableNumericComparison) {
            boolean isNum1 = isNumeric(val1);
            boolean isNum2 = isNumeric(val2);
            
            if (isNum1 && isNum2) {
                Double num1 = parseNumeric(val1);
                Double num2 = parseNumeric(val2);
                
                if (num1 != null && num2 != null) {
                    if (num1 == 0.0 && num2 == 0.0) {
                        return true;
                    }
                    
                    if (num1 == 0.0 || num2 == 0.0) {
                        return Math.abs(num1 - num2) <= numericTolerance;
                    }
                    
                    double relativeError = Math.abs(num1 - num2) / Math.max(Math.abs(num1), Math.abs(num2));
                    return relativeError <= numericTolerance;
                }
            }
        }
        
        return false;
    }
    
    private String formatValueForDisplay(String value) {
        if (value == null || value.isEmpty()) {
            return "(空)";
        }
        
        if (isNumeric(value)) {
            Double num = parseNumeric(value);
            if (num != null) {
                if (SCIENTIFIC_NOTATION.matcher(value.trim()).matches()) {
                    return String.format("%s (= %.10g)", value, num);
                }
                if (Math.abs(num) > 1e6 || (Math.abs(num) < 1e-4 && num != 0)) {
                    return String.format("%s (= %.6e)", value, num);
                }
            }
        }
        
        return value;
    }
    
    public List<Map<String, String>> loadCSV(String filePath) throws IOException {
        logger.info("开始加载CSV文件: " + filePath);
        List<Map<String, String>> data = new ArrayList<>();
        
        try (BufferedReader br = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8)) {
            String headerLine = br.readLine();
            if (headerLine == null) {
                throw new IOException("CSV文件为空");
            }
            
            String[] headers = parseCSVLine(headerLine);
            String line;
            
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                
                String[] values = parseCSVLine(line);
                Map<String, String> row = new LinkedHashMap<>();
                
                for (int i = 0; i < headers.length; i++) {
                    String value = i < values.length ? values[i].trim() : "";
                    row.put(headers[i].trim(), value);
                }
                data.add(row);
            }
        }
        
        logger.info("成功加载 " + data.size() + " 行数据");
        return data;
    }
    
    private String[] parseCSVLine(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
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
    
    private String createCompositeKey(Map<String, String> row) {
        if (primaryKeys.size() == 1) {
            return row.getOrDefault(primaryKeys.get(0), "");
        } else {
            return primaryKeys.stream()
                .map(key -> row.getOrDefault(key, ""))
                .collect(Collectors.joining("|"));
        }
    }
    
    private SampledData intelligentSampling(List<Map<String, String>> data1, List<Map<String, String>> data2) {
        Map<String, Map<String, String>> map1 = new LinkedHashMap<>();
        Map<String, Map<String, String>> map2 = new LinkedHashMap<>();
        
        for (Map<String, String> row : data1) {
            String key = createCompositeKey(row);
            map1.put(key, row);
        }
        
        for (Map<String, String> row : data2) {
            String key = createCompositeKey(row);
            map2.put(key, row);
        }
        
        Set<String> commonKeys = new LinkedHashSet<>(map1.keySet());
        commonKeys.retainAll(map2.keySet());
        
        List<String> keys1Example = map1.keySet().stream().limit(3).collect(Collectors.toList());
        List<String> keys2Example = map2.keySet().stream().limit(3).collect(Collectors.toList());
        logger.info("文件1中的主键示例: " + keys1Example);
        logger.info("文件2中的主键示例: " + keys2Example);
        logger.info("找到 " + commonKeys.size() + " 个共同主键");
        
        if (commonKeys.isEmpty()) {
            logger.warning("未找到共同的主键，使用随机抽样");
            int sampleSize = (int) (Math.min(data1.size(), data2.size()) * sampleRate);
            return new SampledData(
                randomSample(data1, sampleSize),
                randomSample(data2, sampleSize)
            );
        }
        
        List<String> sampledKeys;
        if (sampleRate >= 1.0 || commonKeys.size() <= 100) {
            sampledKeys = new ArrayList<>(commonKeys);
        } else {
            int sampleSize = (int) (commonKeys.size() * sampleRate);
            sampledKeys = randomSample(new ArrayList<>(commonKeys), sampleSize);
        }
        
        List<Map<String, String>> sampled1 = sampledKeys.stream()
            .map(map1::get)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        
        List<Map<String, String>> sampled2 = sampledKeys.stream()
            .map(map2::get)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        
        logger.info("智能抽样完成: " + sampledKeys.size() + " 个主键");
        return new SampledData(sampled1, sampled2);
    }
    
    private <T> List<T> randomSample(List<T> list, int sampleSize) {
        List<T> copy = new ArrayList<>(list);
        Collections.shuffle(copy);
        return copy.subList(0, Math.min(sampleSize, copy.size()));
    }
    
    /**
     * 顺序对比数据（不使用并行）
     */
    private ComparisonResult sequentialCompare(List<Map<String, String>> data1, List<Map<String, String>> data2) {
        
        Map<String, Map<String, String>> map1 = new LinkedHashMap<>();
        Map<String, Map<String, String>> map2 = new LinkedHashMap<>();
        
        for (Map<String, String> row : data1) {
            String key = createCompositeKey(row);
            map1.put(key, row);
        }
        
        for (Map<String, String> row : data2) {
            String key = createCompositeKey(row);
            map2.put(key, row);
        }
        
        Set<String> allKeys = new LinkedHashSet<>(map1.keySet());
        allKeys.addAll(map2.keySet());
        
        Set<String> commonKeys = new LinkedHashSet<>(map1.keySet());
        commonKeys.retainAll(map2.keySet());
        
        Set<String> onlyInData1 = new LinkedHashSet<>(map1.keySet());
        onlyInData1.removeAll(map2.keySet());
        
        Set<String> onlyInData2 = new LinkedHashSet<>(map2.keySet());
        onlyInData2.removeAll(map1.keySet());
        
        ComparisonResult result = new ComparisonResult();
        result.totalKeys = allKeys.size();
        result.commonKeys = commonKeys.size();
        result.onlyInData1 = onlyInData1.size();
        result.onlyInData2 = onlyInData2.size();
        
        // 顺序处理每个主键
        int processedCount = 0;
        for (String key : commonKeys) {
            Map<String, String> row1 = map1.get(key);
            Map<String, String> row2 = map2.get(key);
            
            if (row1 == null || row2 == null) continue;
            
            List<FieldDifference> fieldDiffs = new ArrayList<>();
            
            Set<String> fieldsToCompare = new LinkedHashSet<>();
            fieldsToCompare.addAll(row1.keySet());
            fieldsToCompare.addAll(row2.keySet());
            
            for (String field : fieldsToCompare) {
                if (excludeFields.contains(field)) {
                    continue;
                }
                
                String val1 = row1.getOrDefault(field, "");
                String val2 = row2.getOrDefault(field, "");
                
                if (!valuesEqual(val1, val2)) {
                    boolean isNumericDiff = enableNumericComparison && isNumeric(val1) && isNumeric(val2);
                    fieldDiffs.add(new FieldDifference(field, val1, val2, isNumericDiff));
                    if (isNumericDiff) {
                        result.numericComparisons++;
                    }
                }
            }
            
            if (!fieldDiffs.isEmpty()) {
                result.differences.add(new RowDifference(key, fieldDiffs));
            } else {
                result.identicalRows++;
            }
            
            processedCount++;
            if (processedCount % 1000 == 0) {
                logger.info("已处理 " + processedCount + " / " + commonKeys.size() + " 个主键");
            }
        }
        
        logger.info("对比完成: " + result.commonKeys + " 个共同主键");
        if (enableNumericComparison) {
            logger.info("数值比较次数: " + result.numericComparisons);
        }
        
        return result;
    }
    
    /**
     * 对差异按字段组合进行分组
     */
    private Map<String, List<RowDifference>> groupDifferencesByFields(List<RowDifference> differences) {
        Map<String, List<RowDifference>> groupedDiffs = new LinkedHashMap<>();
        
        for (RowDifference diff : differences) {
            // 生成字段组合的key（排序后保证一致性）
            String fieldGroup = diff.differences.stream()
                .map(fd -> fd.field)
                .sorted()
                .collect(Collectors.joining(", "));
            
            groupedDiffs.computeIfAbsent(fieldGroup, k -> new ArrayList<>()).add(diff);
        }
        
        return groupedDiffs;
    }
    
    public ComparisonResult compareCSVs(String file1, String file2, String outputHtml) throws IOException {
        
        logger.info("开始CSV对比流程");
        if (enableNumericComparison) {
            logger.info("数值比较已启用，容差: " + numericTolerance);
        }
        
        List<Map<String, String>> data1 = loadCSV(file1);
        List<Map<String, String>> data2 = loadCSV(file2);
        
        for (String key : primaryKeys) {
            if (data1.isEmpty() || !data1.get(0).containsKey(key)) {
                throw new IllegalArgumentException("主键 '" + key + "' 不存在于文件1中");
            }
            if (data2.isEmpty() || !data2.get(0).containsKey(key)) {
                throw new IllegalArgumentException("主键 '" + key + "' 不存在于文件2中");
            }
        }
        
        if (!excludeFields.isEmpty()) {
            logger.info("排除比较的字段: " + String.join(", ", excludeFields));
        }
        
        SampledData sampledData = intelligentSampling(data1, data2);
        ComparisonResult result = sequentialCompare(sampledData.data1, sampledData.data2);
        
        generateHTMLReport(result, file1, file2, outputHtml);
        this.comparisonResults = result;
        
        return result;
    }
    
    /**
     * 生成HTML报告（包含按字段组分类）
     */
    private void generateHTMLReport(ComparisonResult result, String file1, String file2, String outputPath) 
            throws IOException {
        
        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html>\n<html>\n<head>\n");
        html.append("    <title>CSV对比报告</title>\n");
        html.append("    <meta charset=\"UTF-8\">\n");
        html.append("    <style>\n");
        html.append("        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }\n");
        html.append("        .header { background-color: #2c3e50; color: white; padding: 20px; border-radius: 5px; }\n");
        html.append("        .summary { margin: 20px 0; background-color: white; padding: 20px; border-radius: 5px; }\n");
        html.append("        .stats { display: flex; justify-content: space-around; margin: 20px 0; flex-wrap: wrap; }\n");
        html.append("        .stat-box { background-color: #e8f4f8; padding: 15px; border-radius: 5px; text-align: center; margin: 5px; min-width: 120px; }\n");
        html.append("        .field-group { margin: 20px 0; background-color: white; padding: 20px; border-radius: 5px; border-left: 4px solid #3498db; }\n");
        html.append("        .group-header { background-color: #3498db; color: white; padding: 10px; border-radius: 3px; margin-bottom: 15px; }\n");
        html.append("        .group-title { font-size: 1.2em; font-weight: bold; }\n");
        html.append("        .group-count { font-size: 0.9em; opacity: 0.9; margin-top: 5px; }\n");
        html.append("        .diff-row { border: 1px solid #ddd; margin: 10px 0; padding: 10px; border-radius: 5px; background-color: #fff; }\n");
        html.append("        .diff-row:nth-child(even) { background-color: #f9f9f9; }\n");
        html.append("        .field-diff { margin: 5px 0; padding: 8px; background-color: #fff3cd; border-radius: 3px; border-left: 3px solid #ffc107; }\n");
        html.append("        .field-diff.numeric { background-color: #e3f2fd; border-left: 3px solid #2196f3; }\n");
        html.append("        .key { font-weight: bold; color: #0066cc; margin-bottom: 10px; }\n");
        html.append("        .field-name { font-weight: bold; color: #333; }\n");
        html.append("        .value1 { color: #d32f2f; font-family: monospace; }\n");
        html.append("        .value2 { color: #388e3c; font-family: monospace; }\n");
        html.append("        .numeric-badge { background-color: #2196f3; color: white; padding: 2px 6px; border-radius: 3px; font-size: 0.8em; margin-left: 5px; }\n");
        html.append("        .no-differences { color: #666; font-style: italic; }\n");
        html.append("        .excluded-fields { background-color: #fff3e0; padding: 10px; border-radius: 5px; margin: 10px 0; border-left: 3px solid #ff9800; }\n");
        html.append("        .config-info { background-color: #e8f5e9; padding: 10px; border-radius: 5px; margin: 10px 0; border-left: 3px solid #4caf50; }\n");
        html.append("        h2 { color: #2c3e50; }\n");
        html.append("        .arrow { color: #666; margin: 0 5px; }\n");
        html.append("        .toc { background-color: #ecf0f1; padding: 15px; border-radius: 5px; margin: 20px 0; }\n");
        html.append("        .toc-item { margin: 5px 0; padding: 5px; cursor: pointer; }\n");
        html.append("        .toc-item:hover { background-color: #bdc3c7; border-radius: 3px; }\n");
        html.append("    </style>\n");
        html.append("</head>\n<body>\n");
        
        html.append("    <div class=\"header\">\n");
        html.append("        <h1>CSV文件对比报告</h1>\n");
        html.append("        <p><strong>文件1:</strong> ").append(escapeHtml(file1)).append("</p>\n");
        html.append("        <p><strong>文件2:</strong> ").append(escapeHtml(file2)).append("</p>\n");
        html.append("        <p><strong>对比时间:</strong> ");
        html.append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        html.append("</p>\n");
        html.append("        <p><strong>主键:</strong> ").append(String.join(", ", primaryKeys)).append("</p>\n");
        
        if (enableNumericComparison) {
            html.append("        <div class=\"config-info\">\n");
            html.append("            <strong>🔢 数值比较已启用</strong><br>\n");
            html.append("            数值容差（相对误差）: ").append(String.format("%.2e", numericTolerance)).append("<br>\n");
            html.append("            自动识别科学表示法和普通数值\n");
            html.append("        </div>\n");
        }
        
        if (!excludeFields.isEmpty()) {
            html.append("        <div class=\"excluded-fields\">\n");
            html.append("            <strong>⚠️ 排除比较的字段:</strong> ").append(String.join(", ", excludeFields)).append("\n");
            html.append("        </div>\n");
        }
        
        html.append("    </div>\n\n");
        
        html.append("    <div class=\"summary\">\n");
        html.append("        <h2>对比摘要</h2>\n");
        html.append("        <div class=\"stats\">\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.totalKeys).append("</h3><p>总主键数</p></div>\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.commonKeys).append("</h3><p>共同主键数</p></div>\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.onlyInData1).append("</h3><p>仅在文件1中</p></div>\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.onlyInData2).append("</h3><p>仅在文件2中</p></div>\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.identicalRows).append("</h3><p>完全相同行数</p></div>\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.differences.size()).append("</h3><p>存在差异行数</p></div>\n");
        if (enableNumericComparison) {
            html.append("            <div class=\"stat-box\"><h3>").append(result.numericComparisons).append("</h3><p>数值字段对比</p></div>\n");
        }
        html.append("        </div>\n");
        html.append("    </div>\n\n");
        
        // 按字段组分类
        Map<String, List<RowDifference>> groupedDiffs = groupDifferencesByFields(result.differences);
        
        html.append("    <div class=\"summary\">\n");
        html.append("        <h2>字段组分类目录</h2>\n");
        html.append("        <div class=\"toc\">\n");
        html.append("            <p>共发现 <strong>").append(groupedDiffs.size()).append("</strong> 种不同的字段组合：</p>\n");
        
        int groupIndex = 0;
        for (Map.Entry<String, List<RowDifference>> entry : groupedDiffs.entrySet()) {
            html.append("            <div class=\"toc-item\">\n");
            html.append("                <a href=\"#group-").append(groupIndex).append("\">📌 ");
            html.append(escapeHtml(entry.getKey())).append(" (").append(entry.getValue().size()).append(" 行)</a>\n");
            html.append("            </div>\n");
            groupIndex++;
        }
        
        html.append("        </div>\n");
        html.append("    </div>\n\n");
        
        // 显示每个字段组的详细差异
        if (groupedDiffs.isEmpty()) {
            html.append("    <div class=\"summary\">\n");
            html.append("        <p class=\"no-differences\">✓ 未发现数据差异</p>\n");
            html.append("    </div>\n");
        } else {
            groupIndex = 0;
            for (Map.Entry<String, List<RowDifference>> entry : groupedDiffs.entrySet()) {
                html.append("    <div class=\"field-group\" id=\"group-").append(groupIndex).append("\">\n");
                html.append("        <div class=\"group-header\">\n");
                html.append("            <div class=\"group-title\">📋 字段组 ").append(groupIndex + 1).append(": ").append(escapeHtml(entry.getKey())).append("</div>\n");
                html.append("            <div class=\"group-count\">包含 ").append(entry.getValue().size()).append(" 行差异</div>\n");
                html.append("        </div>\n");
                
                int limit = Math.min(50, entry.getValue().size());
                for (int i = 0; i < limit; i++) {
                    RowDifference diff = entry.getValue().get(i);
                    html.append("        <div class=\"diff-row\">\n");
                    html.append("            <p class=\"key\">🔑 主键: ").append(escapeHtml(diff.key)).append("</p>\n");
                    
                    for (FieldDifference fieldDiff : diff.differences) {
                        String cssClass = fieldDiff.isNumeric ? "field-diff numeric" : "field-diff";
                        html.append("            <div class=\"").append(cssClass).append("\">\n");
                        html.append("                <span class=\"field-name\">").append(escapeHtml(fieldDiff.field)).append(":</span>");
                        if (fieldDiff.isNumeric) {
                            html.append("<span class=\"numeric-badge\">数值</span>");
                        }
                        html.append("<br>\n");
                        html.append("                <span class=\"value1\">文件1: ").append(escapeHtml(formatValueForDisplay(fieldDiff.value1))).append("</span>\n");
                        html.append("                <span class=\"arrow\">→</span>\n");
                        html.append("                <span class=\"value2\">文件2: ").append(escapeHtml(formatValueForDisplay(fieldDiff.value2))).append("</span>\n");
                        html.append("            </div>\n");
                    }
                    
                    html.append("        </div>\n");
                }
                
                if (entry.getValue().size() > 50) {
                    html.append("        <p class=\"no-differences\">... 还有 ").append(entry.getValue().size() - 50).append(" 条差异未显示</p>\n");
                }
                
                html.append("    </div>\n\n");
                groupIndex++;
            }
        }
        
        // 字段差异统计
        Map<String, Integer> fieldDiffCount = new LinkedHashMap<>();
        Map<String, Integer> numericFieldCount = new LinkedHashMap<>();
        
        for (RowDifference diff : result.differences) {
            for (FieldDifference fieldDiff : diff.differences) {
                if (!excludeFields.contains(fieldDiff.field)) {
                    fieldDiffCount.merge(fieldDiff.field, 1, Integer::sum);
                    if (fieldDiff.isNumeric) {
                        numericFieldCount.merge(fieldDiff.field, 1, Integer::sum);
                    }
                }
            }
        }
        
        html.append("    <div class=\"summary\">\n");
        html.append("        <h2>字段差异统计</h2>\n");
        html.append("        <p>以下字段在对比中发现差异（按差异数量降序）:</p>\n");
        html.append("        <ul>\n");
        
        fieldDiffCount.entrySet().stream()
            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
            .forEach(entry -> {
                html.append("            <li><strong>").append(escapeHtml(entry.getKey())).append("</strong>: ").append(entry.getValue()).append(" 处差异");
                if (numericFieldCount.containsKey(entry.getKey())) {
                    html.append(" <span class=\"numeric-badge\">").append(numericFieldCount.get(entry.getKey())).append(" 次数值比较</span>");
                }
                html.append("</li>\n");
            });
        
        html.append("        </ul>\n");
        html.append("    </div>\n");
        html.append("</body>\n</html>");
        
        Files.write(Paths.get(outputPath), html.toString().getBytes(StandardCharsets.UTF_8));
        logger.info("HTML报告已生成: " + outputPath);
    }
    
    private String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;")
                   .replace("'", "&#39;");
    }
    
    /**
     * 导出差异数据为CSV（包含字段组信息）
     */
    public void exportDifferencesCSV(String outputPath) throws IOException {
        if (comparisonResults == null || comparisonResults.differences.isEmpty()) {
            logger.warning("无差异数据可导出");
            return;
        }
        
        // 按字段组分类
        Map<String, List<RowDifference>> groupedDiffs = groupDifferencesByFields(comparisonResults.differences);
        
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(Paths.get(outputPath), StandardCharsets.UTF_8))) {
            writer.println("field_group,primary_key,field,value_file1,value_file2,is_numeric,numeric_value1,numeric_value2");
            
            for (Map.Entry<String, List<RowDifference>> entry : groupedDiffs.entrySet()) {
                String fieldGroup = entry.getKey();
                
                for (RowDifference diff : entry.getValue()) {
                    for (FieldDifference fieldDiff : diff.differences) {
                        String numVal1 = "";
                        String numVal2 = "";
                        
                        if (fieldDiff.isNumeric) {
                            Double n1 = parseNumeric(fieldDiff.value1);
                            Double n2 = parseNumeric(fieldDiff.value2);
                            if (n1 != null) numVal1 = String.format("%.10e", n1);
                            if (n2 != null) numVal2 = String.format("%.10e", n2);
                        }
                        
                        writer.printf("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",%s,\"%s\",\"%s\"%n",
                            escapeCsv(fieldGroup),
                            escapeCsv(diff.key),
                            escapeCsv(fieldDiff.field),
                            escapeCsv(fieldDiff.value1),
                            escapeCsv(fieldDiff.value2),
                            fieldDiff.isNumeric ? "true" : "false",
                            escapeCsv(numVal1),
                            escapeCsv(numVal2));
                    }
                }
            }
        }
        
        logger.info("差异数据已导出（包含字段组信息）: " + outputPath);
    }
    
    private String escapeCsv(String text) {
        if (text == null) return "";
        return text.replace("\"", "\"\"");
    }
    
    // 内部类
    static class SampledData {
        List<Map<String, String>> data1;
        List<Map<String, String>> data2;
        
        SampledData(List<Map<String, String>> data1, List<Map<String, String>> data2) {
            this.data1 = data1;
            this.data2 = data2;
        }
    }
    
    static class ComparisonResult {
        int totalKeys;
        int commonKeys;
        int onlyInData1;
        int onlyInData2;
        int identicalRows;
        int numericComparisons;
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
        boolean isNumeric;
        
        FieldDifference(String field, String value1, String value2, boolean isNumeric) {
            this.field = field;
            this.value1 = value1;
            this.value2 = value2;
            this.isNumeric = isNumeric;
        }
    }
    
    /**
     * 命令行主函数
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("CSV对比工具 - 支持科学表示法和字段组分类");
            System.out.println("用法: java CSVComparator <文件1> <文件2> [选项]");
            System.out.println("\n必需选项:");
            System.out.println("  --keys, -k <主键1> [主键2...]    主键列名（支持组合主键）");
            System.out.println("\n可选选项:");
            System.out.println("  --exclude, -e <字段1> [字段2...] 排除不比较的字段");
            System.out.println("  --sample-rate, -s <0-1>          抽样率（默认: 0.1）");
            System.out.println("  --output, -o <文件>              输出HTML报告路径（默认: comparison_report.html）");
            System.out.println("  --export-csv <文件>              导出差异数据CSV路径");
            System.out.println("\n数值比较选项:");
            System.out.println("  --numeric                        启用数值比较（支持科学表示法）");
            System.out.println("  --tolerance, -t <数值>           数值相对误差容差（默认: 1e-9）");
            System.out.println("\n示例:");
            System.out.println("  java CSVComparator file1.csv file2.csv --keys ID --numeric --tolerance 0.001");
            System.out.println("  java CSVComparator data1.csv data2.csv -k ID Name -e Timestamp --numeric");
            System.exit(1);
        }
        
        try {
            String file1 = args[0];
            String file2 = args[1];
            List<String> primaryKeys = new ArrayList<>();
            List<String> excludeFields = new ArrayList<>();
            double sampleRate = 0.1;
            String outputHtml = "comparison_report.html";
            String exportCsv = null;
            boolean enableNumeric = false;
            double tolerance = 1e-9;
            
            // 解析选项
            int i = 2;
            while (i < args.length) {
                switch (args[i]) {
                    case "--keys":
                    case "-k":
                        i++;
                        while (i < args.length && !args[i].startsWith("--") && !args[i].startsWith("-")) {
                            primaryKeys.add(args[i++]);
                        }
                        break;
                    case "--exclude":
                    case "-e":
                        i++;
                        while (i < args.length && !args[i].startsWith("--") && !args[i].startsWith("-")) {
                            excludeFields.add(args[i++]);
                        }
                        break;
                    case "--sample-rate":
                    case "-s":
                        sampleRate = Double.parseDouble(args[++i]);
                        i++;
                        break;
                    case "--output":
                    case "-o":
                        outputHtml = args[++i];
                        i++;
                        break;
                    case "--export-csv":
                        exportCsv = args[++i];
                        i++;
                        break;
                    case "--numeric":
                        enableNumeric = true;
                        i++;
                        break;
                    case "--tolerance":
                    case "-t":
                        tolerance = Double.parseDouble(args[++i]);
                        i++;
                        break;
                    default:
                        System.err.println("未知选项: " + args[i]);
                        i++;
                }
            }
            
            // 验证主键参数
            if (primaryKeys.isEmpty()) {
                System.err.println("错误: 必须使用 --keys 指定至少一个主键");
                System.exit(1);
            }
            
            CSVComparator comparator = new CSVComparator(primaryKeys, sampleRate, excludeFields, tolerance, enableNumeric);
            ComparisonResult result = comparator.compareCSVs(file1, file2, outputHtml);
            
            if (exportCsv != null) {
                comparator.exportDifferencesCSV(exportCsv);
            }
            
            // 显示字段组统计
            Map<String, List<RowDifference>> groupedDiffs = comparator.groupDifferencesByFields(result.differences);
            
            System.out.println("\n=== 对比完成 ===");
            System.out.println("总主键数: " + result.totalKeys);
            System.out.println("共同主键数: " + result.commonKeys);
            System.out.println("相同行数: " + result.identicalRows);
            System.out.println("差异行数: " + result.differences.size());
            System.out.println("\n字段组分类: " + groupedDiffs.size() + " 种不同组合");
            
            // 显示前5个字段组
            int count = 0;
            for (Map.Entry<String, List<RowDifference>> entry : groupedDiffs.entrySet()) {
                if (count++ >= 5) break;
                System.out.println("  - [" + entry.getKey() + "]: " + entry.getValue().size() + " 行");
            }
            
            if (groupedDiffs.size() > 5) {
                System.out.println("  ... 还有 " + (groupedDiffs.size() - 5) + " 个字段组");
            }
            
            if (enableNumeric) {
                System.out.println("\n数值字段对比: " + result.numericComparisons);
                System.out.println("数值容差: " + String.format("%.2e", tolerance));
            }
            System.out.println("\nHTML报告: " + outputHtml);
            
        } catch (Exception e) {
            logger.severe("对比过程出错: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
