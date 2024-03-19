package com.aa.rac.mod.codegenerator;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.util.StringUtils;

public class ConverterFileGenerator {

  private static final String pwd = System.getProperty("user.dir").replace('\\', '/');

  private static final String replSourcePath = "./generated/src/main/java/com/aa/rac/mod/domain/converter/";

  private static final String packageName = "com.aa.rac.mod.domain.converter.";

  private String eventHubClassName;

  private String replicatedClassName;

  private String converterClassName;

  private static final String end = ";";
  private FileWriter fileWriter = null;

  private List<String> lines = new ArrayList<>();

  private ReplicatedFileGenerator replicatedFileGenerator;

  private EventHubPojoGenerator eventHubPojoGenerator;

  private DDLSQLFileGenerator ddlsqlFileGenerator;

  private String generatedOutput;

  private Map<String, String> transformationMapper = Map.ofEntries(
      Map.entry("String", "|"),
      Map.entry("Date", "Date.valueOf(RacUtil.getDate(|))"),
      Map.entry("Timestamp", "RacUtil.getSqlTimeFromDb2DateTime(|)"),
      Map.entry("TimestampTicketCreateTs", "RacUtil.getSqlTimeFromDb2DateTime(|, ChronoUnit.SECONDS)"),
      Map.entry("BigDecimal", "RacUtil.convertStringtoBigDecimal(|, $, RoundingMode.UP)"),
      Map.entry("Integer", "Integer.parseInt(|)"),
      Map.entry("BigInteger", "RacUtil.convertStringtoBigInteger(|)")
  );

  public ConverterFileGenerator(ReplicatedFileGenerator replicatedFileGenerator,
                                EventHubPojoGenerator eventHubPojoGenerator,
                                DDLSQLFileGenerator ddlsqlFileGenerator
                                )
      throws IOException {
    this.replicatedFileGenerator = replicatedFileGenerator;
    this.eventHubPojoGenerator = eventHubPojoGenerator;
    this.ddlsqlFileGenerator = ddlsqlFileGenerator;
    this.eventHubClassName = eventHubPojoGenerator.getEventHubImportPath().substring(eventHubPojoGenerator.getEventHubImportPath().lastIndexOf('.')+1);
    this.replicatedClassName = replicatedFileGenerator.getReplicatedImportPath().substring(replicatedFileGenerator.getReplicatedImportPath().lastIndexOf('.')+1);
    this.converterClassName = this.eventHubClassName + "To" + this.replicatedClassName + "Converter";
  }

  public String getGeneratedOutput() {
    return generatedOutput;
  }

  public String getConverterDirectory() {
    return pwd + replSourcePath.substring(1);
  }

  public String getConverterImportPath() {
    return packageName + converterClassName;
  }

  public String getFullConverterFilePath() {
    return getConverterDirectory() + "/" + converterClassName + ".java.txt";
  }

  public FileWriter getFileWriter(String fullFilePath) throws IOException {
    if (fileWriter == null) {
      fileWriter = new FileWriter(fullFilePath);
    }
    return fileWriter;
  }

  public void addPackageContents(String packageName) {
    lines.add("package " + packageName.substring(0, packageName.length()-1) + end + "\n\n");
  }

  public void addImportStatements(String eventHubImportPath, String replicatedImportPath) throws IOException {
    String imports = "import com.aa.rac.mod.domain.util.RacUtil;\n" +
        "import " + replicatedImportPath + ";\n" +
        "import " + eventHubImportPath + ";\n" +
        "import java.math.RoundingMode;\n" +
        "import java.sql.Date;\n" +
        "import java.sql.Timestamp;\n" +
        "import java.time.temporal.ChronoUnit;\n" +
        "import org.jetbrains.annotations.NotNull;\n" +
        "import org.slf4j.Logger;\n" +
        "import org.slf4j.LoggerFactory;\n" +
        "import org.springframework.core.convert.converter.Converter;";
    lines.add(imports +"\n\n");
  }

  public void addClassJavaDoc() {
    lines.add("/** " + eventHubClassName + " to " + replicatedClassName + " converter. */\n");
  }

  public void addClassAnnotation() {
    lines.add("@SuppressWarnings(\"checkstyle:LineLength\")\n");
  }

  public void addInitialClassTemplate(String className) {
    lines.add("public class " + converterClassName
        + "\n    implements Converter<" + eventHubClassName + ", "
        + replicatedClassName + "> {\n\n");
  }

  public String getMethodAnnotation() {
    return "@Override";
  }

  public void addClassFields() {
    lines.add("  protected final Logger logger = LoggerFactory.getLogger(this.getClass());\n\n");
  }

  public void addFields(boolean isBefore) {
    String before = isBefore?"Before":"";
    for (String uuidColumnName: ddlsqlFileGenerator.uuidColumnNames) {
      lines.add("\n          target.set"
          +StringUtils.capitalize(
          FileUtil.getFieldName(uuidColumnName)) + "(#TODO: GENERATE HASH);");
    }
    for (String field : replicatedFileGenerator.getJson().keySet()) {
      if (field.startsWith("B_") || replicatedFileGenerator.ehBaseColumnsSet.contains(field)) {
        continue;
      }
      String fieldUp = StringUtils.capitalize(FileUtil.getFieldName(field))+before;
      String sourceField = "source.get" + fieldUp + "()";
      String transformation = null;
      if (field.equalsIgnoreCase("TICKET_CREATE_TS")) {
        transformation = transformationMapper.get("TimestampTicketCreateTs");
      } else {
        transformation = transformationMapper.get(
            replicatedFileGenerator.getDb2DataTypeMap().get(
                FileUtil.truncatedDataType(
                    ddlsqlFileGenerator.getDataTypeMap().get(field))));
      }
      if (transformation == null) {
        throw new IllegalArgumentException("Cannot get transformation for field type: " + field);
      }
      transformation = transformation.replace("|", sourceField);
      if (FileUtil.truncatedDataType(
          ddlsqlFileGenerator.getDataTypeMap().get(field)).equalsIgnoreCase("decimal")) {
        transformation = transformation.replace("$",
            FileUtil.getDecimalPrecision(
                ddlsqlFileGenerator.getDataTypeMap().get(field)));
      }
      lines.add("\n          target.set" + StringUtils.capitalize(FileUtil.getFieldName(field)) + "("
          + transformation + ");");
    }
  }

  public void addMethod() {
    lines.add("  " + getMethodAnnotation());
    lines.add("\n  public " + replicatedClassName + " convert(@NotNull " + eventHubClassName + " source) {\n    "
    + replicatedClassName + " target = new " + replicatedClassName
        + "();\n    try { " );
    lines.add("\n      if (source.getEntityType() == null) {\n" +
        "        throw new IllegalArgumentException(\"EntityType in eventhub pojo cannot be null\");\n" +
        "      }");
    lines.add("\n      switch (source.getEntityType()) {\n        case \"PT\", \"UP\", \"RR\" -> {");
    addFields(false);
    lines.add("\n        }");
    lines.add("\n        case \"DL\" -> {");
    addFields(true);
    lines.add("\n        }\n" +
        "        default -> throw new IllegalStateException(\n" +
        "            \"Unexpected value: \" + source.getEntityType());\n" +
        "      }\n" +
        "    } catch (Exception ex) {\n" +
        "      logger.error(\"Error occured in "+eventHubClassName.toLowerCase()+" convertor {} \", ex.getMessage());\n" +
        "      ex.printStackTrace();\n" +
        "      throw new RuntimeException(ex);\n" +
        "    }\n" +
        "    return target;\n" +
        "  }\n");
  }

  public void addEndingLine() {
    lines.add("}");
  }

  public void generateConverterFile() throws IOException {
    String fullPath = getFullConverterFilePath();
    FileUtil.createFile(getConverterDirectory(), fullPath);
    FileWriter writer = getFileWriter(fullPath);
    addPackageContents(packageName);
    addImportStatements(eventHubPojoGenerator.getEventHubImportPath(), replicatedFileGenerator.getReplicatedImportPath());
    addClassJavaDoc();
    addClassAnnotation();
    addInitialClassTemplate(converterClassName);
    addClassFields();
    addMethod();
    addEndingLine();
    this.generatedOutput = String.join("", lines);
    try {
      writer.write(this.generatedOutput);
    } finally {
      writer.close();
    }
    System.out.println("Converter file successfully generated. Please review at location: " + fullPath);
    System.out.println("\tNOTE: Please review the generated code.");
  }
}
