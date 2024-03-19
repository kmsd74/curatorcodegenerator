package com.aa.rac.mod.codegenerator;

import com.aa.rac.mod.CuratorcodegeneratorApplication;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReplicatedFileGenerator {

  private static final String pwd = System.getProperty("user.dir").replace('\\', '/');

  private static final String replSourcePath = "./generated/src/main/java/com/aa/rac/mod/orm/dao/";

  private static final String packageName = "com.aa.rac.mod.orm.dao.";

  private String tableName;

  private static final String end = ";";
  private FileWriter fileWriter = null;

  private List<String> lines = new ArrayList<>();

  public Map<String, Object> json = new LinkedHashMap<>();

  private String unpackedTrim = "UnpackingNestedStringTrimDeserializer";
  private String unpacked = "";

  private String trimmed = "TrimmingDeserializer";

  public Set<String> ehBaseColumnsSet = new HashSet<>(Arrays.asList("A_ENTTYP", "A_TIMSTAMP", "A_USER", "A_JOBUSER"));

  private Map<String, String> db2DataTypeMap =
      Map.ofEntries(
          Map.entry("CHAR", "String"),
          Map.entry("DATE", "Date"),
          Map.entry("TIMESTMP", "Timestamp"),
          Map.entry("TIMESTAMP", "Timestamp"),
          Map.entry("VC", "String"),
          Map.entry("VARCHAR", "String"),
          Map.entry("DECIMAL", "BigDecimal"),
          Map.entry("SMALLINT", "Integer"),
          Map.entry("BIGINT", "BigInteger"),
          Map.entry("INTEGER", "Integer"),
          Map.entry("INT", "Integer")
      );

  private String replicatedClassName;

  public DDLSQLFileGenerator ddlsqlFileGenerator;

  private String filePath;
  private String generatedOutput;

  public Map<String, String> columnTypes = new LinkedHashMap<>();

  public List<String> skipColumns = Arrays.asList("TICKET_CREATE_TS", "A_ENTTYP", "A_TIMSTAMP", "A_USER", "A_JOBUSER");



  public ReplicatedFileGenerator(String filePath, DDLSQLFileGenerator ddlsqlFileGenerator) {
    this.filePath = filePath;
    this.ddlsqlFileGenerator = ddlsqlFileGenerator;
    String eventHubClassName = FileUtil.getClassName(filePath);
    replicatedClassName = eventHubClassName + "Repl";
    this.tableName = eventHubClassName.toLowerCase();
  }

  public Map<String, Object> getJson() {
    return this.json;
  }

  public void validateJson() {
    for (String field: json.keySet()) {
      field = field.startsWith("B_") ? field.substring(2) : field;
      if (skipColumns.contains(field)) {
        continue;
      }
      if (ddlsqlFileGenerator.getDataTypeMap().get(field) == null) {
        throw new IllegalArgumentException("Field not found in DDL script provided.");
      }
      if (db2DataTypeMap.get(FileUtil.truncatedDataType(ddlsqlFileGenerator.getDataTypeMap().get(field)).toUpperCase()) == null) {
        throw new IllegalArgumentException("Data type not mapped b/w DDL type and Java type in ReplicatedFileGenerator.");
      }
    }
  }

  public Map<String, Object> getJson(String jsonString) throws JsonProcessingException {
    if (this.json.isEmpty()) {
      this.json = FileUtil.mapContentsToHashMap(jsonString);
      validateJson();
    }
    return this.json;
  }

  public Map<String, String> getDb2DataTypeMap() {
    return db2DataTypeMap;
  }

  public String getGeneratedOutput() {
    return generatedOutput;
  }

  public String getReplicatedDirectory() {
    return pwd + replSourcePath.substring(1);
  }

  public String getReplicatedImportPath() {
    return packageName + replicatedClassName.toLowerCase() + "." + replicatedClassName;
  }

  public String getFullReplicatedFilePath(String fileName) {
    return getReplicatedDirectory() + tableName +"repl/" + fileName;
  }

  public FileWriter getFileWriter(String fullFilePath) throws IOException {
    if (fileWriter == null) {
      fileWriter = new FileWriter(fullFilePath);
    }
    return fileWriter;
  }

  public void addPackageContents(String packageName) throws IOException {
    lines.add("package " + packageName + tableName + "repl" + end + "\n\n");
  }

  public void addImportStatements() throws IOException {
    String imports = "import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;\n" +
        "import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;\n" +
        "\n" +
        "import com.aa.rac.mod.domain.util.RacUtil;\n" +
        "import com.aa.rac.mod.orm.dao.AbstractCuratedBaseEntity;\n" +
        "import com.fasterxml.jackson.annotation.JsonAutoDetect;\n" +
        "import com.fasterxml.jackson.annotation.JsonFormat;\n" +
        "import com.fasterxml.jackson.annotation.JsonProperty;\n" +
        "import jakarta.persistence.Column;\n" +
        "import jakarta.persistence.Entity;\n" +
        "import jakarta.persistence.Id;\n" +
        "import jakarta.persistence.Table;\n" +
        "import java.math.BigDecimal;\n" +
        "import java.math.BigInteger;\n" +
        "import java.sql.Date;\n" +
        "import java.sql.Timestamp;\n" +
        "import lombok.Getter;\n" +
        "import lombok.NoArgsConstructor;\n" +
        "import lombok.Setter;\n" +
        "import lombok.ToString;";
    lines.add(imports +"\n\n");
  }

  public void addClassJavaDoc() {
    lines.add("/** " + replicatedClassName + " replicated orm. */\n");
  }

  public void addClassAnnotations() throws IOException {
    String annotations = "@Entity\n" +
        "@Table(name = \"" +ddlsqlFileGenerator.tableName+ "\", schema = \""
        + CuratorcodegeneratorApplication.SCHEMA_NAME + "\")\n" +
        "@JsonAutoDetect(fieldVisibility = ANY, getterVisibility = NONE, setterVisibility = NONE)\n" +
        "@NoArgsConstructor\n" +
        "@ToString(callSuper = true)\n" +
        "@Setter\n" +
        "@Getter\n";
    lines.add(annotations);
  }

  public void addInitialClassTemplate(String className) throws IOException {
    lines.add("public class " + className + " extends AbstractCuratedBaseEntity { \n\n");
  }

  public Set<String> getFieldNames(String jsonString) throws JsonProcessingException {
    return getJson(jsonString).keySet();
  }

  public String getColumnAnnotation(String field) {
    if (!ddlsqlFileGenerator.uuidColumnNames.contains(field) && !ddlsqlFileGenerator.getNullMap().containsKey(field)) {
      throw new IllegalArgumentException("Field not found: " + field);
    }
    String nullable;
    if (ddlsqlFileGenerator.uuidColumnNames.contains(field)) {
      nullable = ", nullable = false";
    } else {
      nullable = ddlsqlFileGenerator.getNullMap().get(field).isBlank()?"":", nullable = false";
    }

    return "@Column(name = \"" + field + "\"" + nullable + ")\n";
  }

  public String getJsonPropertyAnnotation(String field) {
    if (!ddlsqlFileGenerator.uuidColumnNames.contains(field) && !ddlsqlFileGenerator.getNullMap().containsKey(field)) {
      throw new IllegalArgumentException("Field not found: " + field);
    }
    return "@JsonProperty(\"" + field + "\")\n";
  }

  public String getJsonFormatAnnotation(String field) {
    return "@JsonFormat(pattern = \"yyyy-MM-dd\")\n";
  }

  public String getIdAnnotation() {
    return "@Id\n";
  }

  public void addFields() {
    String pk = ddlsqlFileGenerator.uuidColumnNames.get(0);
    for (String uuidColumnName: ddlsqlFileGenerator.uuidColumnNames) {
      if (uuidColumnName.equals(pk)) {
        lines.add("  " + getIdAnnotation());
      }
      lines.add("  " + getColumnAnnotation(uuidColumnName));
      lines.add("  " + getJsonPropertyAnnotation(uuidColumnName));
      columnTypes.put(uuidColumnName, "String");
      lines.add("  private String " + FileUtil.getFieldName(uuidColumnName) + ";\n\n");
    }
    for (Map.Entry<String, Object> entry: json.entrySet()) {
      String field = entry.getKey();
      String value = entry.getValue().toString();
      if (ehBaseColumnsSet.contains(field) || field.startsWith("B_")) {
        continue;
      }
      lines.add("  " + getColumnAnnotation(field));
      lines.add("  " + getJsonPropertyAnnotation(field));
      if (ddlsqlFileGenerator.getDataTypeMap().get(field).equalsIgnoreCase("date")) {
        lines.add("  " + getJsonFormatAnnotation(field));
      }
      if (field.equalsIgnoreCase("TICKET_CREATE_TS")) {
        columnTypes.put(field, "Timestamp");
        lines.add("  private Timestamp " + FileUtil.getFieldName(field) +";\n\n");
        continue;
      }
      columnTypes.put(field, db2DataTypeMap.get(FileUtil.truncatedDataType(ddlsqlFileGenerator.getDataTypeMap().get(field))));
      lines.add("  private " + db2DataTypeMap.get(FileUtil.truncatedDataType(ddlsqlFileGenerator.getDataTypeMap().get(field)))
          + " " + FileUtil.getFieldName(field) +";\n\n");
    }
  }

  public void addGetUniqueIdentifier() {
    lines.add("  @Override\n");
    lines.add("  public String getUniqueIdentifier() { \n    return " +
        FileUtil.getFieldName(ddlsqlFileGenerator.uuidColumnNames.get(0)) +"; \n  }\n");
  }

  public void addEndingLine() {
    lines.add("}");
  }

  public void generateReplicatedFile() throws IOException {
    String replicatedClassFileName = replicatedClassName + ".java.txt";
    Set<String> fieldNames = getFieldNames(String.join("", FileUtil.readLinesFromFile(filePath)));
    String fullPath = getFullReplicatedFilePath(replicatedClassFileName);
    FileUtil.createFile(getReplicatedDirectory() + tableName +"repl", fullPath);
    FileWriter writer = getFileWriter(fullPath);
    addPackageContents(packageName);
    addImportStatements();
    addClassJavaDoc();
    addClassAnnotations();
    addInitialClassTemplate(replicatedClassName);
    addFields();
    addGetUniqueIdentifier();
    addEndingLine();
    this.generatedOutput = String.join("", lines);
    try {
      writer.write(this.generatedOutput);
    } finally {
      writer.close();
    }
    System.out.println("Replicated file successfully generated. Please review at location: " + fullPath);
    System.out.println("\tNOTE: Please review uuid column and modify data types of other columns.");
  }
}
