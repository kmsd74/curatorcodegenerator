package com.aa.rac.mod.codegenerator;

import com.aa.rac.mod.CuratorcodegeneratorApplication;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ValidatorYamlFileGenerator {

  private int fieldDataTypeGapLength = 30;
  private static final String pwd = System.getProperty("user.dir").replace('\\', '/');

  private static final String replSourcePath = "./generated/src/test/resources/";

  public String tableName;

  private static final String end = ";";
  private FileWriter fileWriter = null;

  private List<String> lines = new ArrayList<>();

  private Map<String, Object> json = new LinkedHashMap<>();

  private Map<String, String> dataTypeMap = new HashMap<>();

  public Map<String, String> nullMap = new HashMap<>();

  public Map<String, Boolean> trimMap = new HashMap<>();

  private Map<String, String> db2ToPgdataTypeMap = Map.ofEntries(
      Map.entry("CHAR", "VARCHAR"),
      Map.entry("DATE", "DATE"),
      Map.entry("TIMESTMP", "TIMESTAMP WITHOUT TIME ZONE"),
      Map.entry("TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE"),
      Map.entry("VARCHAR", "VARCHAR"),
      Map.entry("VC", "VARCHAR"),
      Map.entry("DECIMAL", "DECIMAL"),
      Map.entry("SMALLINT", "SMALLINT"),
      Map.entry("BIGINT", "BIGINT"),
      Map.entry("INTEGER", "INTEGER"),
      Map.entry("INT", "INTEGER")
  );

  private Set<String> db2DataTypeSet = db2ToPgdataTypeMap.keySet();

  private String filePath;

  private String generatedOutput;

  private String fileName;

  public ValidatorYamlFileGenerator(String fileName, String filePath, String tableName) {
    this.fileName = fileName;
    this.filePath = filePath;
    this.tableName = tableName.toLowerCase();
  }

  public String getGeneratedOutput() {
    return generatedOutput;
  }

  public Map<String, String> getDataTypeMap() {
    return dataTypeMap;
  }

  public Map<String, String> getNullMap() {
    return nullMap;
  }

  public Map<String, Boolean> getTrimMap() {
    return trimMap;
  }

  public void loadDataTypeAndNullAndTrimMaps() {
    if (dataTypeMap.isEmpty()) {
      for (Map.Entry<String, Object> entry: json.entrySet()) {
        String field = entry.getKey();
        String[] properties = entry.getValue().toString().split("[|]");
        String nullable = properties.length>1?properties[1]:"";
        String value = properties[0];
        dataTypeMap.put(field, value);
        nullMap.put(field, nullable);
        Boolean trimmable = properties.length>2?properties[2].equalsIgnoreCase("y"):false;
        trimMap.put(field, trimmable);
      }
    }
  }

  public Map<String, Object> getJson(String jsonString) throws JsonProcessingException {
    if (this.json.isEmpty()) {
      this.json = FileUtil.mapContentsToHashMap(jsonString);
      loadDataTypeAndNullAndTrimMaps();
    }
    return this.json;
  }

  public String getValidatorDirectory() {
    return pwd + replSourcePath.substring(1);
  }

  public String getFullValidatordFilePath(String fileName) {
    return getValidatorDirectory() + fileName;
  }

  public FileWriter getFileWriter(String fullFilePath) throws IOException {
    if (fileWriter == null) {
      fileWriter = new FileWriter(fullFilePath);
    }
    return fileWriter;
  }

  public void addCreateStatement() {
    lines.add("CREATE TABLE IF NOT EXISTS " + CuratorcodegeneratorApplication.SCHEMA_NAME  +"." + tableName +"\n(\n");
  }

  public Set<String> getFieldNames(String jsonString) throws JsonProcessingException {
    return getJson(jsonString).keySet();
  }

  public void writeAuditColumns() {
    lines.add("  ignorePostgresColumns: \n");
    lines.add("    - SRC_DELETED_INDICATOR "+ "\n");
    lines.add("    - DELETED_INDICATOR "+ "\n");
    lines.add("    - DML_FLG "+ "\n");
    lines.add("    - EVENTHUB_TIMESTAMP "+ "\n");
    lines.add("    - SYSTEM_MODIFIED_TIMESTAMP "+ "\n");
    lines.add("    - CREATED_BY "+ "\n");
    lines.add("    - MODIFIED_BY "+ "\n");
    lines.add("    - VERSION "+ "\n");
    lines.add("  deleteIndicatorColumns: \n");
    lines.add("    - DELETED_INDICATOR "+ "\n");
    lines.add("    - SRC_DELETED_INDICATOR "+ "\n");
  }

  public void addFields() {
    lines.add("  columnProperties: {\n");
    int counter = 0;
    int lastIndex = json.entrySet().size();
    for (Map.Entry<String, Object> entry: json.entrySet()) {
      String field = entry.getKey();
      String[] properties = entry.getValue().toString().split("[|]");
      String nullableProperty = properties.length>1?properties[1]:"";
      String nullable = "";
      if(nullableProperty.equalsIgnoreCase("NOT NULL"))
      {
        nullable = "No";
      }
      else
      {
        nullable = "Yes";
      }
      String trimmableProperty = properties.length>1?properties[2]:"";
      String trimmable = "";
      if(trimmableProperty.equals("N"))
      {
        trimmable = "No";
      }
      else if(trimmableProperty.equalsIgnoreCase("Y"))
      {
        trimmable = "Yes";
      }
      String value = properties[0];
      String dataType = value.lastIndexOf("(") == -1 ? value : value.substring(0, value.lastIndexOf("("));
      if (!db2DataTypeSet.contains(dataType)) {
        throw new IllegalArgumentException("Data type not matched. Please check the data types for " + field + ": " + entry.getValue().toString());
      } 
      counter++;
      if(counter < lastIndex)
      {
      lines.add("    "+field + ": {"
          + "datatype: " + db2ToPgdataTypeMap.get(dataType)+ (value.lastIndexOf("(")==-1 || value.toLowerCase().startsWith("timest")?"":value.substring(value.lastIndexOf("("))) + ", trimmable: " + trimmable + ", nullable: " +nullable +"},\n");
      }
      else
      {
        lines.add("    "+field + ": {"
          + "datatype: " + db2ToPgdataTypeMap.get(dataType)+ (value.lastIndexOf("(")==-1 || value.toLowerCase().startsWith("timest")?"":value.substring(value.lastIndexOf("("))) + ", trimmable: " + trimmable + ", nullable: " +nullable +"} \n }");
      }
        }

  }

  public void generateValidatorYamlFile() throws IOException {
    String validatorFileName = "application-" + fileName + ".yml";
    Set<String> fieldNames = getFieldNames(String.join("", FileUtil.readLinesFromFile(filePath)));
    String fullPath = getFullValidatordFilePath(validatorFileName);
    FileUtil.createFile(getValidatorDirectory(), fullPath);
    FileWriter writer = getFileWriter(fullPath);
    lines.add(fileName + ":\n");
    lines.add("  tableShortName: " + fileName.toUpperCase() + "\n");
    lines.add("  servicePath: " + "/ServicePath"+ "\n");
    lines.add("  blobFolderName: " + fileName + "Reports"+ "\n");
    lines.add("  sampleN: " + "1"+ "\n");
    lines.add("  postgresKeys: \n");
    lines.add("    - KEY "+ "\n");
    lines.add("  db2Keys: "+ "\n");
    lines.add("    DB2_COLUMN: " + "POST_GRECOLUMN"+ "\n");
    writeAuditColumns();
    addFields();
    this.generatedOutput = String.join("", lines);
    try {
      writer.write(this.generatedOutput);
    } finally {
      writer.close();
    }
    System.out.println("Validator YAML file successfully generated. Please review at location: " + fullPath);
    System.out.println("\tNOTE: Please review the generated YAML and modify validation columns, db2keys, ignore, and delete indicators as needed.");
  }
}
