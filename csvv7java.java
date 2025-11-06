import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * High-Performance CSV Comparison Tool - Java 8 Version
 * Modified: Removed parallel processing, added field grouping
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
     * Load large CSV files in chunks
     */
    public List<Map<String, String>> loadCSV(String filePath) throws IOException {
        log("Loading CSV file: " + filePath);
        List<Map<String, String>> data = new ArrayList<>();
        
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8))) {
            
            String headerLine = br.readLine();
            if (headerLine == null) {
                throw new IOException("CSV file is empty");
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
        
        log("Successfully loaded " + data.size() + " rows");
        return data;
    }
    
    /**
     * Parse CSV line (handle quotes and commas)
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
     * Create composite primary key
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
     * Normalize numeric values (handle scientific notation, ensure positive/negative consistency)
     * Treats values like 0.0, .00, 0.00, 0 as identical
     */
    private String normalizeValue(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        
        // Try to parse as numeric value
        try {
            // Remove leading/trailing spaces
            value = value.trim();
            
            // Check if it's a number (including scientific notation)
            if (value.matches("^[+-]?\\d+\\.?\\d*[eE][+-]?\\d+$") || 
                value.matches("^[+-]?\\.?\\d+\\.?\\d*$")) {
                
                BigDecimal bd = new BigDecimal(value);
                
                // Compare with zero to normalize 0.0, .00, 0.00, etc. to "0"
                if (bd.compareTo(BigDecimal.ZERO) == 0) {
                    return "0";
                }
                
                // Convert to plain string and remove trailing zeros
                return bd.stripTrailingZeros().toPlainString();
            }
        } catch (NumberFormatException e) {
            // Not a number, return original value
        }
        
        return value;
    }
    
    /**
     * Intelligent sampling strategy
     */
    public SampledData intelligentSampling(List<Map<String, String>> data1, 
                                          List<Map<String, String>> data2) {
        
        // Create primary key mapping
        Map<String, Map<String, String>> map1 = data1.stream()
                .collect(Collectors.toMap(this::createCompositeKey, row -> row, (a, b) -> a));
        
        Map<String, Map<String, String>> map2 = data2.stream()
                .collect(Collectors.toMap(this::createCompositeKey, row -> row, (a, b) -> a));
        
        // Find common keys
        Set<String> commonKeys = new HashSet<>(map1.keySet());
        commonKeys.retainAll(map2.keySet());
        
        log("Found " + commonKeys.size() + " common primary keys");
        
        if (commonKeys.isEmpty()) {
            log("Warning: No common primary keys found");
            return new SampledData(new ArrayList<>(), new ArrayList<>(), new HashSet<>());
        }
        
        // Sample
        Set<String> sampledKeys;
        if (sampleRate >= 1.0 || commonKeys.size() <= 100) {
            sampledKeys = commonKeys;
        } else {
            int sampleSize = (int) (commonKeys.size() * sampleRate);
            List<String> keyList = new ArrayList<>(commonKeys);
            Collections.shuffle(keyList);
            sampledKeys = new HashSet<>(keyList.subList(0, sampleSize));
        }
        
        // Filter data
        List<Map<String, String>> sampled1 = sampledKeys.stream()
                .map(map1::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        
        List<Map<String, String>> sampled2 = sampledKeys.stream()
                .map(map2::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        
        log("Intelligent sampling completed: " + sampledKeys.size() + " keys");
        
        return new SampledData(sampled1, sampled2, sampledKeys);
    }
    
    /**
     * Sequential data comparison (removed parallel processing)
     */
    public ComparisonResult sequentialCompare(List<Map<String, String>> data1,
                                             List<Map<String, String>> data2) {
        
        // Create index
        Map<String, Map<String, String>> indexed1 = data1.stream()
                .collect(Collectors.toMap(this::createCompositeKey, row -> row, (a, b) -> a));
        
        Map<String, Map<String, String>> indexed2 = data2.stream()
                .collect(Collectors.toMap(this::createCompositeKey, row -> row, (a, b) -> a));
        
        // Calculate common keys
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
        
        // Sequential comparison
        log("Starting sequential comparison of " + commonKeys.size() + " common keys");
        
        int processed = 0;
        for (String key : commonKeys) {
            Map<String, String> row1 = indexed1.get(key);
            Map<String, String> row2 = indexed2.get(key);
            
            if (row1 == null || row2 == null) {
                continue;
            }
            
            List<FieldDifference> fieldDiffs = new ArrayList<>();
            
            // Compare each field
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
                result.differences.add(new RowDifference(key, fieldDiffs, row1, row2));
            } else {
                result.identicalRows++;
            }
            
            processed++;
            if (processed % 1000 == 0) {
                log("Processed " + processed + " / " + commonKeys.size() + " rows");
            }
        }
        
        // Group differences by field combinations
        result.fieldGroups = groupDifferencesByFields(result.differences);
        
        log("Comparison completed: " + result.commonKeys + " common keys, " + 
            result.differences.size() + " differences found");
        log("Field combinations found: " + result.fieldGroups.size());
        
        return result;
    }
    
    /**
     * Group row differences by field combinations
     */
    private Map<String, FieldGroup> groupDifferencesByFields(List<RowDifference> differences) {
        Map<String, FieldGroup> groups = new LinkedHashMap<>();
        
        for (RowDifference diff : differences) {
            // Create field combination key (sorted for consistency)
            List<String> fields = diff.differences.stream()
                    .map(fd -> fd.field)
                    .sorted()
                    .collect(Collectors.toList());
            
            String groupKey = String.join(", ", fields);
            
            FieldGroup group = groups.get(groupKey);
            if (group == null) {
                group = new FieldGroup(fields);
                groups.put(groupKey, group);
            }
            
            group.addDifference(diff);
        }
        
        // Sort groups by count (descending)
        return groups.entrySet().stream()
                .sorted((a, b) -> Integer.compare(b.getValue().count, a.getValue().count))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> a,
                        LinkedHashMap::new
                ));
    }
    
    /**
     * Compare CSV files
     */
    public ComparisonResult compareCSVs(String file1, String file2, String outputHtml) 
            throws IOException {
        
        log("Starting CSV comparison process");
        
        // Load data
        List<Map<String, String>> data1 = loadCSV(file1);
        List<Map<String, String>> data2 = loadCSV(file2);
        
        // Validate primary keys
        for (String key : primaryKeys) {
            if (!data1.get(0).containsKey(key)) {
                throw new IllegalArgumentException("Primary key '" + key + "' not found in file1");
            }
            if (!data2.get(0).containsKey(key)) {
                throw new IllegalArgumentException("Primary key '" + key + "' not found in file2");
            }
        }
        
        if (!excludeFields.isEmpty()) {
            log("Excluded fields: " + String.join(", ", excludeFields));
        }
        
        // Intelligent sampling
        SampledData sampled = intelligentSampling(data1, data2);
        
        // Execute sequential comparison
        ComparisonResult result = sequentialCompare(sampled.data1, sampled.data2);
        
        // Generate HTML report
        generateHTMLReport(result, file1, file2, outputHtml);
        
        this.comparisonResults = result;
        return result;
    }
    
    /**
     * Generate HTML report with field grouping
     */
    private void generateHTMLReport(ComparisonResult result, String file1, 
                                   String file2, String outputPath) throws IOException {
        
        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html>\n<html>\n<head>\n");
        html.append("    <title>CSV Comparison Report</title>\n");
        html.append("    <meta charset=\"UTF-8\">\n");
        html.append("    <style>\n");
        html.append("        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }\n");
        html.append("        .header { background-color: #2c3e50; color: white; padding: 20px; border-radius: 5px; margin-bottom: 20px; }\n");
        html.append("        .stats { display: flex; justify-content: space-around; margin: 20px 0; flex-wrap: wrap; }\n");
        html.append("        .stat-box { background-color: #3498db; color: white; padding: 15px; border-radius: 5px; text-align: center; margin: 10px; min-width: 150px; }\n");
        html.append("        .field-group { border: 2px solid #3498db; margin: 20px 0; padding: 20px; border-radius: 8px; background-color: white; }\n");
        html.append("        .group-header { background-color: #3498db; color: white; padding: 15px; border-radius: 5px; margin-bottom: 15px; }\n");
        html.append("        .group-fields { font-size: 1.2em; font-weight: bold; margin: 10px 0; }\n");
        html.append("        .group-count { font-size: 1.1em; color: #e74c3c; }\n");
        html.append("        .diff-row { border: 1px solid #ddd; margin: 10px 0; padding: 15px; border-radius: 5px; background-color: #f9f9f9; }\n");
        html.append("        .diff-row:nth-child(even) { background-color: #ecf0f1; }\n");
        html.append("        .field-diff { margin: 5px 0; padding: 8px; background-color: #fff3cd; border-radius: 3px; border-left: 4px solid #ffc107; }\n");
        html.append("        .key { font-weight: bold; color: #2c3e50; font-size: 1.1em; margin-bottom: 10px; }\n");
        html.append("        .value1 { color: #e74c3c; font-weight: bold; }\n");
        html.append("        .value2 { color: #27ae60; font-weight: bold; }\n");
        html.append("        .full-row { margin-top: 15px; padding: 10px; background-color: #ecf0f1; border-radius: 3px; }\n");
        html.append("        .full-row h4 { margin: 5px 0; color: #2c3e50; }\n");
        html.append("        .row-data { font-family: 'Courier New', monospace; font-size: 0.9em; white-space: pre-wrap; word-break: break-all; }\n");
        html.append("        .row-file1 { background-color: #ffebee; padding: 8px; border-radius: 3px; margin: 5px 0; border-left: 4px solid #e74c3c; }\n");
        html.append("        .row-file2 { background-color: #e8f5e9; padding: 8px; border-radius: 3px; margin: 5px 0; border-left: 4px solid #27ae60; }\n");
        html.append("        .field-label { display: inline-block; min-width: 150px; font-weight: bold; }\n");
        html.append("        .summary-section { background-color: white; padding: 20px; border-radius: 5px; margin: 20px 0; }\n");
        html.append("        .toggle-btn { background-color: #3498db; color: white; border: none; padding: 8px 15px; border-radius: 4px; cursor: pointer; margin: 5px 0; }\n");
        html.append("        .toggle-btn:hover { background-color: #2980b9; }\n");
        html.append("        .details { display: none; margin-top: 10px; }\n");
        html.append("        .details.show { display: block; }\n");
        html.append("    </style>\n");
        html.append("    <script>\n");
        html.append("        function toggleDetails(id) {\n");
        html.append("            var details = document.getElementById(id);\n");
        html.append("            details.classList.toggle('show');\n");
        html.append("        }\n");
        html.append("    </script>\n");
        html.append("</head>\n<body>\n");
        
        // Header
        html.append("    <div class=\"header\">\n");
        html.append("        <h1>CSV Comparison Report</h1>\n");
        html.append("        <p><strong>File 1:</strong> ").append(escapeHtml(file1)).append("</p>\n");
        html.append("        <p><strong>File 2:</strong> ").append(escapeHtml(file2)).append("</p>\n");
        html.append("        <p><strong>Comparison Time:</strong> ").append(
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("</p>\n");
        html.append("        <p><strong>Primary Keys:</strong> ").append(String.join(", ", primaryKeys)).append("</p>\n");
        
        if (!excludeFields.isEmpty()) {
            html.append("        <p><strong>Excluded Fields:</strong> ").append(
                    String.join(", ", excludeFields)).append("</p>\n");
        }
        
        html.append("    </div>\n");
        
        // Overall Stats
        html.append("    <div class=\"summary-section\">\n");
        html.append("        <h2>Comparison Summary</h2>\n");
        html.append("        <div class=\"stats\">\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.totalKeys)
            .append("</h3><p>Total Keys</p></div>\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.commonKeys)
            .append("</h3><p>Common Keys</p></div>\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.onlyInData1)
            .append("</h3><p>Only in File 1</p></div>\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.onlyInData2)
            .append("</h3><p>Only in File 2</p></div>\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.identicalRows)
            .append("</h3><p>Identical Rows</p></div>\n");
        html.append("            <div class=\"stat-box\"><h3>").append(result.differences.size())
            .append("</h3><p>Rows with Differences</p></div>\n");
        html.append("        </div>\n");
        html.append("    </div>\n");
        
        // Field Groups
        html.append("    <div class=\"summary-section\">\n");
        html.append("        <h2>Differences Grouped by Field Combinations</h2>\n");
        html.append("        <p>Total field combinations: <strong>").append(result.fieldGroups.size()).append("</strong></p>\n");
        
        if (result.fieldGroups.isEmpty()) {
            html.append("        <p>No differences found</p>\n");
        } else {
            int groupIndex = 0;
            for (Map.Entry<String, FieldGroup> entry : result.fieldGroups.entrySet()) {
                groupIndex++;
                FieldGroup group = entry.getValue();
                String groupId = "group_" + groupIndex;
                
                html.append("        <div class=\"field-group\">\n");
                html.append("            <div class=\"group-header\">\n");
                html.append("                <div class=\"group-fields\">Fields: ").append(escapeHtml(entry.getKey())).append("</div>\n");
                html.append("                <div class=\"group-count\">Number of rows with these differences: ").append(group.count).append("</div>\n");
                html.append("                <button class=\"toggle-btn\" onclick=\"toggleDetails('").append(groupId).append("')\">Show/Hide Details</button>\n");
                html.append("            </div>\n");
                
                html.append("            <div id=\"").append(groupId).append("\" class=\"details\">\n");
                
                // Show first 50 differences in this group
                int limit = Math.min(50, group.differences.size());
                for (int i = 0; i < limit; i++) {
                    RowDifference diff = group.differences.get(i);
                    
                    html.append("                <div class=\"diff-row\">\n");
                    html.append("                    <p class=\"key\">Primary Key: ").append(escapeHtml(diff.key)).append("</p>\n");
                    
                    // Show field differences
                    html.append("                    <div style=\"margin: 10px 0;\">\n");
                    for (FieldDifference fd : diff.differences) {
                        html.append("                        <div class=\"field-diff\">\n");
                        html.append("                            <strong>").append(escapeHtml(fd.field)).append(":</strong><br>\n");
                        html.append("                            <span class=\"value1\">File 1: ").append(escapeHtml(fd.value1)).append("</span><br>\n");
                        html.append("                            <span class=\"value2\">File 2: ").append(escapeHtml(fd.value2)).append("</span>\n");
                        html.append("                        </div>\n");
                    }
                    html.append("                    </div>\n");
                    
                    // Show complete rows
                    html.append("                    <div class=\"full-row\">\n");
                    html.append("                        <h4>Complete Row from File 1:</h4>\n");
                    html.append("                        <div class=\"row-file1 row-data\">\n");
                    for (Map.Entry<String, String> rowEntry : diff.completeRow1.entrySet()) {
                        html.append("                            <span class=\"field-label\">").append(escapeHtml(rowEntry.getKey())).append(":</span> ");
                        html.append(escapeHtml(rowEntry.getValue())).append("<br>\n");
                    }
                    html.append("                        </div>\n");
                    
                    html.append("                        <h4>Complete Row from File 2:</h4>\n");
                    html.append("                        <div class=\"row-file2 row-data\">\n");
                    for (Map.Entry<String, String> rowEntry : diff.completeRow2.entrySet()) {
                        html.append("                            <span class=\"field-label\">").append(escapeHtml(rowEntry.getKey())).append(":</span> ");
                        html.append(escapeHtml(rowEntry.getValue())).append("<br>\n");
                    }
                    html.append("                        </div>\n");
                    html.append("                    </div>\n");
                    
                    html.append("                </div>\n");
                }
                
                if (group.differences.size() > limit) {
                    html.append("                <p style=\"text-align: center; color: #7f8c8d; margin: 15px 0;\">");
                    html.append("Showing ").append(limit).append(" of ").append(group.differences.size()).append(" differences in this group");
                    html.append("</p>\n");
                }
                
                html.append("            </div>\n");
                html.append("        </div>\n");
            }
        }
        
        html.append("    </div>\n");
        
        // Field Statistics
        html.append("    <div class=\"summary-section\">\n");
        html.append("        <h2>Individual Field Difference Statistics</h2>\n");
        html.append("        <p>Fields ranked by number of differences:</p>\n");
        html.append("        <ul>\n");
        
        // Analyze field difference patterns
        Map<String, Integer> fieldDiffCount = new HashMap<>();
        for (RowDifference diff : result.differences) {
            for (FieldDifference fd : diff.differences) {
                if (!excludeFields.contains(fd.field)) {
                    fieldDiffCount.put(fd.field, fieldDiffCount.getOrDefault(fd.field, 0) + 1);
                }
            }
        }
        
        fieldDiffCount.entrySet().stream()
                .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                .forEach(entry -> {
                    html.append("            <li><strong>").append(escapeHtml(entry.getKey()))
                        .append("</strong>: ").append(entry.getValue()).append(" differences</li>\n");
                });
        
        html.append("        </ul>\n");
        html.append("    </div>\n");
        
        html.append("</body>\n</html>");
        
        Files.write(Paths.get(outputPath), html.toString().getBytes(StandardCharsets.UTF_8));
        log("HTML report generated: " + outputPath);
    }
    
    private String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;");
    }
    
    private void log(String message) {
        System.out.println("[" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "] " + message);
    }
    
    // Inner classes
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
        Map<String, FieldGroup> fieldGroups = new LinkedHashMap<>();
    }
    
    static class RowDifference {
        String key;
        List<FieldDifference> differences;
        Map<String, String> completeRow1;
        Map<String, String> completeRow2;
        
        RowDifference(String key, List<FieldDifference> differences, 
                     Map<String, String> completeRow1, Map<String, String> completeRow2) {
            this.key = key;
            this.differences = differences;
            this.completeRow1 = completeRow1;
            this.completeRow2 = completeRow2;
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
    
    static class FieldGroup {
        List<String> fields;
        List<RowDifference> differences;
        int count;
        
        FieldGroup(List<String> fields) {
            this.fields = fields;
            this.differences = new ArrayList<>();
            this.count = 0;
        }
        
        void addDifference(RowDifference diff) {
            this.differences.add(diff);
            this.count++;
        }
    }
    
    /**
     * Main method
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java CSVComparator <file1> <file2> <key1[,key2,...]> [options]");
            System.out.println("Options:");
            System.out.println("  --sample-rate=<rate>  Sample rate (0-1), default 0.1");
            System.out.println("  --exclude=<field1,field2,...>  Excluded fields");
            System.out.println("  --output=<path>  Output HTML path, default comparison_report.html");
            System.out.println("\nExample:");
            System.out.println("  java CSVComparator file1.csv file2.csv id --sample-rate=0.2 --output=report.html");
            return;
        }
        
        String file1 = args[0];
        String file2 = args[1];
        List<String> keys = Arrays.asList(args[2].split(","));
        
        double sampleRate = 0.1;
        List<String> excludeFields = new ArrayList<>();
        String output = "comparison_report.html";
        
        // Parse arguments
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
            
            System.out.println("\n=== Comparison Complete ===");
            System.out.println("Total keys: " + result.totalKeys);
            System.out.println("Common keys: " + result.commonKeys);
            System.out.println("Identical rows: " + result.identicalRows);
            System.out.println("Rows with differences: " + result.differences.size());
            System.out.println("Field combinations: " + result.fieldGroups.size());
            System.out.println("\nField Combination Details:");
            for (Map.Entry<String, FieldGroup> entry : result.fieldGroups.entrySet()) {
                System.out.println("  " + entry.getKey() + ": " + entry.getValue().count + " rows");
            }
            System.out.println("\nHTML report: " + output);
            
        } catch (Exception e) {
            System.err.println("Error during comparison: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
