package com.aa.rac.mod.codegenerator;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AdhocDeleteServiceFileGenerator {

  private static final String pwd = System.getProperty("user.dir").replace('\\', '/');

  private static final String replSourcePath = "./generated/src/main/java/com/aa/rac/mod/service/adhocdeletes/agmmtkts/";

  private static final String packageName = "com.aa.rac.mod.service.adhocdeletes.agmmtkts.";

  private String eventHubClassName;

  private String adhocDeleteEventhubClassName = "AgmmtktsAdhocDelete";
  private String adhocDeleteEventhubPojoClassMapper = "AGMMTKTS_ADHOC_DELETE";

  private String repoVar;

  private String replicatedClassName;

  private String repositoryClassName;

  private String serviceClassName;

  private static final String end = ";";
  private FileWriter fileWriter = null;

  private List<String> lines = new ArrayList<>();

  public ReplicatedFileGenerator replicatedFileGenerator;

  public EventHubPojoGenerator eventHubPojoGenerator;

  public RepositoryFileGenerator repositoryFileGenerator;

  private String generatedOutput;

  public String serviceClassMapper;
  public String eventHubClassMapper;
  public String replicatedClassMapper;

  public AdhocDeleteServiceFileGenerator(ReplicatedFileGenerator replicatedFileGenerator,
                                         EventHubPojoGenerator eventHubPojoGenerator,
                                         RepositoryFileGenerator repositoryFileGenerator) {
    this.replicatedFileGenerator = replicatedFileGenerator;
    this.eventHubPojoGenerator = eventHubPojoGenerator;
    this.repositoryFileGenerator = repositoryFileGenerator;
    this.eventHubClassName = eventHubPojoGenerator.getEventHubImportPath().substring(eventHubPojoGenerator.getEventHubImportPath().lastIndexOf('.')+1);
    this.replicatedClassName = replicatedFileGenerator.getReplicatedImportPath().substring(replicatedFileGenerator.getReplicatedImportPath().lastIndexOf('.')+1);
    this.repositoryClassName = repositoryFileGenerator.getRepositoryImportPath().substring(repositoryFileGenerator.getRepositoryImportPath().lastIndexOf('.')+1);
    this.serviceClassName = this.eventHubClassName + "AdhocDeleteServiceImpl";
    this.repoVar = repositoryClassName.substring(0,1).toLowerCase() + repositoryClassName.substring(1);
  }

  public String getAdhocDeleteEventhubImportPath() {
    return eventHubPojoGenerator.getEventHubImportPath().substring(0, eventHubPojoGenerator.getEventHubImportPath().lastIndexOf('.')+1) + adhocDeleteEventhubClassName;
  }

  public String getGeneratedOutput() {
    return generatedOutput;
  }

  public String getServiceDirectory() {
    return pwd + replSourcePath.substring(1);
  }

  public String getServiceImportPath() {
    return packageName + replicatedClassName.toLowerCase() + "." + serviceClassName;
  }

  public String getFullServiceFilePath() {
    return getServiceDirectory() + "/" + serviceClassName + ".java.txt";
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

  public void addImportStatements(String eventHubImportPath,
                                  String replicatedImportPath,
                                  String repositoryImportPath) throws IOException {
    String imports = "import com.aa.rac.mod.domain.annotations.EmitToEventHub;\n" +
        "import com.aa.rac.mod.domain.annotations.SetAuditColumns;\n" +
        "import com.aa.rac.mod.domain.annotations.SetServiceClasses;\n" +
        "import com.aa.rac.mod.domain.enums.CuratedEntityClassMapper;\n" +
        "import com.aa.rac.mod.domain.enums.EventHubPojoClassMapper;\n" +
        "import com.aa.rac.mod.domain.exceptions.QueueException;\n" +
        "import " + replicatedImportPath + ";\n" +
        "import " + eventHubImportPath + ";\n" +
        "import " + repositoryImportPath + ";\n" +
        "import com.aa.rac.mod.service.abstracts.AbstractAdhocDeleteService;\n" +
            "import java.util.Optional;\n"+
        "import java.util.concurrent.Future;\n" +
            "import org.springframework.beans.factory.annotation.Autowired;\n" +
        "import org.springframework.scheduling.annotation.Async;\n" +
        "import org.springframework.scheduling.annotation.EnableAsync;\n" +
        "import org.springframework.stereotype.Service;\n";
    lines.add(imports +"\n\n");
  }

  public void addClassJavaDoc() {
    lines.add("/** " + serviceClassName + " class. */\n");
  }

  public String getClassAnnotations() {
    return "@Service(\""+serviceClassName+"\")\n" +
        "@EnableAsync\n" +
        "@SetAuditColumns(targetCuratedCdcTimestampField = \"eventHubTimestamp\")\n" +
        "@SetServiceClasses(eventHubClassMapper = EventHubPojoClassMapper." + adhocDeleteEventhubPojoClassMapper + ",\n" +
        "    curatedTargetClassMapper = CuratedEntityClassMapper." + replicatedClassName.toUpperCase() + ",\n" +
        "    repoClass = " + repositoryClassName + ".class\n" +
        ")\n" +
        "@EmitToEventHub(topicPropertyName = \"spring.kafka.topics.emit."+eventHubClassName.toLowerCase()+"\")\n"+
        "@SuppressWarnings(\"checkstyle:LineLength\")\n";
  }

  public void addInitialClassTemplate(String className) {
    lines.add(getClassAnnotations());
    lines.add("public class " + serviceClassName
        + " \n    extends AbstractAdhocDeleteService<" + adhocDeleteEventhubClassName + ", "
        + replicatedClassName + ", " + repositoryClassName + "> {\n");
  }

  public void addFields() {
    lines.add("\n  @Autowired private "+repositoryClassName+" "+repoVar+";\n");
  }

  public void addExistingDbRowMethod() {
    lines.add("\n  @Override\n" +
            "  protected Optional<"+replicatedClassName+"> getExistingDbRow(AgmmtktsAdhocDelete adhocDelete) {\n" +
            "    return "+repositoryClassName.substring(0,1).toLowerCase()+repositoryClassName.substring(1)+".findByTicketUuid(adhocDelete.getUniqueIdentifier());\n" +
            "  }\n");
  }

  public String getMethodAnnotation() {
    return "\n  @Override\n" + "  @Async";
  }

  public void addMethod() {
    addExistingDbRowMethod();
    lines.add("  " + getMethodAnnotation());
    lines.add("\n  public Future<String> processAsync(String topicPayload) throws QueueException { \n");
    lines.add("    return processPayload(topicPayload);\n" +
        "  }\n");
  }

  public void addEndingLine() {
    lines.add("}");
  }

  public void generateConverterFile() throws IOException {
    String fullPath = getFullServiceFilePath();
    FileUtil.createFile(getServiceDirectory(), fullPath);
    FileWriter writer = getFileWriter(fullPath);
    addPackageContents(packageName);
    addImportStatements(getAdhocDeleteEventhubImportPath(),
        replicatedFileGenerator.getReplicatedImportPath(),
        repositoryFileGenerator.getRepositoryImportPath());
    addClassJavaDoc();
    addInitialClassTemplate(serviceClassName);
    addFields();
    addMethod();
    addEndingLine();
    this.generatedOutput = String.join("", lines);
//    System.out.println(this.generatedOutput);
    try {
      writer.write(this.generatedOutput);
    } finally {
      writer.close();
    }
    System.out.println("AdhocDeleteService file successfully generated. Please review at location: " + fullPath);
    System.out.println("\tNOTE: Please review the generated code.");
  }
}
