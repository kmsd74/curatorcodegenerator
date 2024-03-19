package com.aa.rac.mod.codegenerator;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;

@Getter
public class EventHubPojoGenerator {

  private static final String pwd = System.getProperty("user.dir").replace('\\', '/');

  private static final String ehSourcePath = "./generated/src/main/java/com/aa/rac/mod/repository/eventhub/";

  private static final String packageName = "com.aa.rac.mod.repository.eventhub";

  private static final String end = ";";
  private FileWriter fileWriter = null;

  private List<String> lines = new ArrayList<>();

  private Map<String, Object> json = new LinkedHashMap<>();

  private String unpackedTrim = "UnpackingNestedStringTrimDeserializer";
  private String repacked = "RepackNestedStringSerializer";
  private String unpacked = "";

  private String trimmed = "TrimmingDeserializer";

  private Set<String> ehBaseColumnsSet = new HashSet<>(Arrays.asList("A_ENTTYP", "A_TIMSTAMP", "A_USER", "A_JOBUSER"));

  private String eventHubClassName;

  private DDLSQLFileGenerator ddlsqlFileGenerator;

  private String filePath;
  private String generatedOutput;

  private boolean filterDefaultTicketCreateTs;


  public EventHubPojoGenerator(String filePath, DDLSQLFileGenerator ddlsqlFileGenerator, boolean filterDefaultTicketCreateTs) {
    this.filePath = filePath;
    eventHubClassName = FileUtil.getClassName(filePath);
    this.ddlsqlFileGenerator = ddlsqlFileGenerator;
    this.filterDefaultTicketCreateTs = filterDefaultTicketCreateTs;
  }

  public String getGeneratedOutput() {
    return generatedOutput;
  }

  public Map<String, Object> getJson(String jsonString) throws JsonProcessingException {
    if (this.json.isEmpty()) {
      this.json = FileUtil.mapContentsToHashMap(jsonString);
    }
    return this.json;
  }

  public String getEventHubDirectory() {
    return pwd + ehSourcePath.substring(1);
  }

  public String getFullEventHubFilePath(String fileName) {
    return getEventHubDirectory() + fileName;
  }

  public FileWriter getFileWriter(String fullFilePath) throws IOException {
    if (fileWriter == null) {
      fileWriter = new FileWriter(fullFilePath);
    }
    return fileWriter;
  }

  public void addPackageContents(String packageName) throws IOException {
    lines.add("package " + packageName + end + "\n\n");
  }

  public void addImportStatements() throws IOException {
    String imports = "import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;\n" +
        "import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;\n\n" +
        "import com.aa.rac.mod.domain.serialization.RepackNestedStringSerializer;\n" +
        "import com.aa.rac.mod.domain.serialization.TrimmingDeserializer;\n" +
        "import com.aa.rac.mod.domain.serialization.UnpackingNestedStringTrimDeserializer;\n" +
        "import com.fasterxml.jackson.annotation.JsonAutoDetect;\n" +
        "import com.fasterxml.jackson.annotation.JsonIgnoreProperties;\n" +
        "import com.fasterxml.jackson.annotation.JsonProperty;\n" +
        "import com.fasterxml.jackson.databind.annotation.JsonDeserialize;\n" +
        "import com.fasterxml.jackson.databind.annotation.JsonSerialize;\n" +
        "import lombok.Getter;\n" +
        "import lombok.NoArgsConstructor;\n" +
        "import lombok.Setter;\n" +
        "import lombok.ToString;";
    lines.add(imports +"\n\n");
  }

  public void addClassJavaDoc() {
    lines.add("/** " + eventHubClassName + " eventhub pojo. */");
  }

  public void addClassAnnotations() throws IOException {
    String annotations = "\n@JsonIgnoreProperties(ignoreUnknown = true)\n" +
        "@JsonAutoDetect(fieldVisibility = ANY, getterVisibility = NONE, setterVisibility = NONE)\n" +
        "@NoArgsConstructor\n" +
        "@ToString(callSuper = true)\n" +
        "@Setter\n" +
        "@Getter\n";
    lines.add(annotations);
  }

  public void addInitialClassTemplate(String className) throws IOException {
    lines.add("public class " + className + " extends AbstractEventHubBaseEntity { \n\n");
  }

  public String getEventHubImportPath() {
    return packageName + "." + eventHubClassName;
  }

  public Set<String> getFieldNames(String jsonString) throws JsonProcessingException {
    return getJson(jsonString).keySet();
  }

  public String getJsonPropertyAnnotation(String field) {
    return "@JsonProperty(\"" + field + "\")\n";
  }

  public String getJsonDeserializeAnnotation(String className) {
    return "@JsonDeserialize(using = " + className + ".class)\n";
  }

  public String getJsonSerializeAnnotation(String className) {
    return "@JsonSerialize(using = " + className + ".class)\n";
  }

  public void addFields() {
    for (Map.Entry<String, Object> entry: json.entrySet()) {
      String field = entry.getKey();
      String value = entry.getValue().toString();
      if (ehBaseColumnsSet.contains(field)) {
        continue;
      }
      lines.add("  " + getJsonPropertyAnnotation(field));
      String fieldUp = field.startsWith("B_")?field.substring(2):field;
      if (ddlsqlFileGenerator.getTrimMap().get(fieldUp)) {

        if (value.contains("{")) {
          lines.add("  " + getJsonDeserializeAnnotation(unpackedTrim));
          lines.add("  " + getJsonSerializeAnnotation(repacked));
        } else {
          lines.add("  " + getJsonDeserializeAnnotation(trimmed));
        }
      }

      lines.add("  private String " + FileUtil.getFieldName(field) +";\n\n");
    }
  }

  public void addFilterDefaultTicketCreateTsMethod() {
    lines.add("\n  /** Check if any tickets have default ticket_create_ts \"0001-01-01T00:00:00.00000\". */\n" +
            "  public boolean filterDefaultTicketCreateTs() {\n" +
            "    if (entityType.equals(\"DL\")) {\n" +
            "      return (this.ticketCreateTsBefore != null\n" +
            "              && this.ticketCreateTsBefore.startsWith(\"0001-01-01\"));\n" +
            "    }\n" +
            "    return (this.ticketCreateTs != null\n" +
            "            && this.ticketCreateTs.startsWith(\"0001-01-01\"));\n" +
            "  }\n\n");
  }



  public void addEndingLine() {
    lines.add("}");
  }

  public void eventHubPojoFileGenerator() throws IOException {
    String eventHubClassFileName = FileUtil.getClassFileName(filePath);
    Set<String> fieldNames = getFieldNames(String.join("", FileUtil.readLinesFromFile(filePath)));
    String fullPath = getFullEventHubFilePath(eventHubClassFileName);
    FileUtil.createFile(getEventHubDirectory(), fullPath);
    FileWriter writer = getFileWriter(fullPath);
    addPackageContents(packageName);
    addImportStatements();
    addClassJavaDoc();
    addClassAnnotations();
    addInitialClassTemplate(eventHubClassName);
    addFields();
    if (this.filterDefaultTicketCreateTs) {
      addFilterDefaultTicketCreateTsMethod();
    }
    addEndingLine();
    this.generatedOutput = String.join("", lines);
    try {
      writer.write(this.generatedOutput);
    } finally {
      writer.close();
    }
    System.out.println("EventHub Pojo successfully generated. Please review at location: " + fullPath);
  }
}
