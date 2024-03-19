package com.aa.rac.mod.codegenerator;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.util.StringUtils;

public class ServiceFileGenerator {

  private static final String pwd = System.getProperty("user.dir").replace('\\', '/');

  private static final String replSourcePath = "./generated/src/main/java/com/aa/rac/mod/service/";

  private static final String packageName = "com.aa.rac.mod.service.";

  private String eventHubClassName;

  private String replicatedClassName;

  private String repositoryClassName;

  private String serviceClassName;

  private static final String end = ";";
  private FileWriter fileWriter = null;

  private List<String> lines = new ArrayList<>();

  public ReplicatedFileGenerator replicatedFileGenerator;

  public EventHubPojoGenerator eventHubPojoGenerator;

  public RepositoryFileGenerator repositoryFileGenerator;

  public boolean filterDefaultTicketCreatets;

  private String generatedOutput;

  public String serviceClassMapper;
  public String eventHubClassMapper;
  public String replicatedClassMapper;

  public ServiceFileGenerator(ReplicatedFileGenerator replicatedFileGenerator,
                              EventHubPojoGenerator eventHubPojoGenerator,
                              RepositoryFileGenerator repositoryFileGenerator,
                              boolean filterDefaultTicketCreatets) {
    this.replicatedFileGenerator = replicatedFileGenerator;
    this.eventHubPojoGenerator = eventHubPojoGenerator;
    this.repositoryFileGenerator = repositoryFileGenerator;
    this.eventHubClassName = eventHubPojoGenerator.getEventHubImportPath().substring(eventHubPojoGenerator.getEventHubImportPath().lastIndexOf('.')+1);
    this.replicatedClassName = replicatedFileGenerator.getReplicatedImportPath().substring(replicatedFileGenerator.getReplicatedImportPath().lastIndexOf('.')+1);
    this.repositoryClassName = repositoryFileGenerator.getRepositoryImportPath().substring(repositoryFileGenerator.getRepositoryImportPath().lastIndexOf('.')+1);
    this.serviceClassName = this.replicatedClassName + "ServiceImpl";
    this.filterDefaultTicketCreatets = filterDefaultTicketCreatets;
  }

  public String getGeneratedOutput() {
    return generatedOutput;
  }

  public String getServiceDirectory() {
    return pwd + replSourcePath.substring(1)+replicatedClassName.toLowerCase();
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
    lines.add("package " + packageName + replicatedClassName.toLowerCase() + end + "\n\n");
  }

  public void addImportStatements(String eventHubImportPath,
                                  String replicatedImportPath,
                                  String repositoryImportPath) throws IOException {
    String imports = "import com.aa.rac.mod.domain.annotations.Filter;\n" +
        "import com.aa.rac.mod.domain.annotations.EmitToEventHub;\n" +
        "import com.aa.rac.mod.domain.annotations.SetAuditColumns;\n" +
        "import com.aa.rac.mod.domain.annotations.SetServiceClasses;\n" +
        "import com.aa.rac.mod.domain.enums.CuratedEntityClassMapper;\n" +
        "import com.aa.rac.mod.domain.enums.EventHubPojoClassMapper;\n" +
        "import com.aa.rac.mod.domain.exceptions.QueueException;\n" +
        "import " + replicatedImportPath + ";\n" +
        "import " + eventHubImportPath + ";\n" +
        "import " + repositoryImportPath + ";\n" +
        "import com.aa.rac.mod.service.abstracts.AbstractServiceEventHub;\n" +
        "import java.util.concurrent.Future;\n" +
        "import org.springframework.scheduling.annotation.Async;\n" +
        "import org.springframework.scheduling.annotation.EnableAsync;\n" +
        "import org.springframework.stereotype.Service;\n";
    lines.add(imports +"\n\n");
  }

  public void addClassJavaDoc() {
    lines.add("/** " + replicatedClassName + " service implementation. */\n");
  }

  public String getClassAnnotations() {
    String s = "@Service(\""+serviceClassName+"\")\n" +
            "@EnableAsync\n" +
            "@SetAuditColumns(targetCuratedCdcTimestampField = \"eventHubTimestamp\")\n" +
            "@SetServiceClasses(eventHubClassMapper = EventHubPojoClassMapper." + eventHubClassName.toUpperCase() + ",\n" +
            "    curatedTargetClassMapper = CuratedEntityClassMapper." + replicatedClassName.toUpperCase() + ",\n" +
            "    repoClass = " + repositoryClassName + ".class\n" +
            ")\n";
    if (filterDefaultTicketCreatets) {
      s += "@Filter(\n" +
              "        clazz = "+eventHubClassName+".class,\n" +
              "        methodName = \"filterDefaultTicketCreateTs\",\n" +
              "        throwDeadLetterQueueException = false,\n" +
              "        referenceType = Filter.ClassType.SOURCE)\n";
    }
    s += "@EmitToEventHub(topicPropertyName = \"spring.kafka.topics.emit."+eventHubClassName.toLowerCase()+"\")\n"+
            "@SuppressWarnings(\"checkstyle:LineLength\")\n";
    return s;
  }

  public void addInitialClassTemplate(String className) {
    lines.add(getClassAnnotations());
    lines.add("public class " + serviceClassName
        + " \n    extends AbstractServiceEventHub<" + eventHubClassName + ", "
        + replicatedClassName + ", " + repositoryClassName + "> {\n");
  }

  public String getMethodAnnotation() {
    return "\n  @Override\n" + "  @Async";
  }

  public void addMethod() {
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
    addImportStatements(eventHubPojoGenerator.getEventHubImportPath(),
        replicatedFileGenerator.getReplicatedImportPath(),
        repositoryFileGenerator.getRepositoryImportPath());
    addClassJavaDoc();
    addInitialClassTemplate(serviceClassName);
    addMethod();
    addEndingLine();
    this.generatedOutput = String.join("", lines);
//    System.out.println(this.generatedOutput);
    try {
      writer.write(this.generatedOutput);
    } finally {
      writer.close();
    }
    System.out.println("Service file successfully generated. Please review at location: " + fullPath);
    System.out.println("\tNOTE: Please review the generated code.");
  }
}
