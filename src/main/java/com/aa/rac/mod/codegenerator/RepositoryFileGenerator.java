package com.aa.rac.mod.codegenerator;

import com.aa.rac.mod.CuratorcodegeneratorApplication;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.springframework.util.StringUtils;

public class RepositoryFileGenerator {

  private static final String pwd = System.getProperty("user.dir").replace('\\', '/');

  private static final String replSourcePath = "./generated/src/main/java/com/aa/rac/mod/repository/findatahub/";

  private static final String packageName = "com.aa.rac.mod.repository.findatahub.";

  private String replicatedClassName;

  private String repositoryClassName;

  private static final String end = ";";
  private FileWriter fileWriter = null;

  private List<String> lines = new ArrayList<>();
  private String generatedOutput;

  public String uuidColumnName;

  private DDLSQLFileGenerator ddlsqlFileGenerator;


  public RepositoryFileGenerator(DDLSQLFileGenerator ddlsqlFileGenerator, String replicatedClassName) {
    this.replicatedClassName = replicatedClassName;
    this.repositoryClassName = this.replicatedClassName + "Repository";
    this.ddlsqlFileGenerator = ddlsqlFileGenerator;
    this.uuidColumnName = ddlsqlFileGenerator.uuidColumnNames.get(0);
  }

  public String getGeneratedOutput() {
    return generatedOutput;
  }

  public String getRepositoryDirectory() {
    return pwd + replSourcePath.substring(1);
  }

  public String getRepositoryImportPath() {
    return packageName + replicatedClassName.toLowerCase() + "." + repositoryClassName;
  }

  public String getFullRepositoryFilePath() {
    return getRepositoryDirectory() + replicatedClassName.toLowerCase() + "/" + repositoryClassName + ".java.txt";
  }

  public FileWriter getFileWriter(String fullFilePath) throws IOException {
    if (fileWriter == null) {
      fileWriter = new FileWriter(fullFilePath);
    }
    return fileWriter;
  }

  public void addPackageContents(String packageName) throws IOException {
    lines.add("package " + packageName + replicatedClassName.toLowerCase() + end + "\n\n");
  }

  public void addImportStatements(String replicatedImportPath) throws IOException {
    String imports = "import " + replicatedImportPath + ";\n" +
        "import java.util.Optional;\n" +
        "import org.springframework.data.jpa.repository.JpaRepository;\n" +
        "import org.springframework.data.jpa.repository.Query;\n" +
        "import org.springframework.stereotype.Repository;";
    lines.add(imports +"\n\n");
  }

  public void addClassJavaDoc() {
    lines.add("/** " + replicatedClassName + " repository. */\n");
  }

  public void addClassAnnotations() throws IOException {
    String annotations = "@Repository\n";
    lines.add(annotations);
  }

  public void addInitialClassTemplate(String className) throws IOException {
    lines.add("public interface " + className
        + " extends JpaRepository<"
        + replicatedClassName + ", String>" + " { \n\n");
  }
//@Query(
//      value = "SELECT * FROM curated.processed_refund WHERE refunded_amount_uuid=?1",
//      nativeQuery = true)
//  Optional<ProcessedRefund> findByRefundedAmountUuid(String refundedAmountUuid);

  public String getQueryAnnotation(String uuidColumnName) {
    return "@Query(value = \"SELECT * FROM "+ CuratorcodegeneratorApplication.SCHEMA_NAME + "."
        + ddlsqlFileGenerator.tableName
        + "\"\n                + \" WHERE " + uuidColumnName.toLowerCase() + "=?1\", " +
        "\n                  nativeQuery = true)\n";
  }

  public void addMethod(String uuidColumnName) {
    lines.add("  " + getQueryAnnotation(uuidColumnName));
    String fieldName = FileUtil.getFieldName(uuidColumnName);
    lines.add("  " + "Optional<"+replicatedClassName+"> findBy"
        + StringUtils.capitalize(fieldName)
        + "(String " + fieldName + ");\n\n");
  }

  public void addEndingLine() {
    lines.add("}");
  }

  public void generateRepositoryFile(String replicatedImportPath) throws IOException {
    String fullPath = getFullRepositoryFilePath();
    FileUtil.createFile(getRepositoryDirectory() + replicatedClassName.toLowerCase(), fullPath);
    FileWriter writer = getFileWriter(fullPath);
    addPackageContents(packageName);
    addImportStatements(replicatedImportPath);
    addClassJavaDoc();
    addClassAnnotations();
    addInitialClassTemplate(repositoryClassName);
    for (String uuidColName: ddlsqlFileGenerator.uuidColumnNames) {
      addMethod(uuidColName);
    }
    addEndingLine();
    this.generatedOutput = String.join("", lines);
    try {
      writer.write(this.generatedOutput);
    } finally {
      writer.close();
    }
    System.out.println("Repository file successfully generated. Please review at location: " + fullPath);
    System.out.println("\tNOTE: Please review the generated code.");
  }
}
