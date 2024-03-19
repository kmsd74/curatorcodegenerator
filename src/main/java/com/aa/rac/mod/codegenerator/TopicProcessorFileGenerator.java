package com.aa.rac.mod.codegenerator;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TopicProcessorFileGenerator {

  private static final String pwd = System.getProperty("user.dir").replace('\\', '/');

  private static final String replSourcePath = "./generated/src/main/java/com/aa/rac/mod/resources/kafka";

  private static final String packageName = "com.aa.rac.mod.resources.kafka.";

  private String eventHubClassName;

  private String replicatedClassName;

  private String repositoryClassName;

  private String processorClassName;

  private static final String end = ";";
  private FileWriter fileWriter = null;

  private List<String> lines = new ArrayList<>();

  private ReplicatedFileGenerator replicatedFileGenerator;

  private EventHubPojoGenerator eventHubPojoGenerator;

  private String generatedOutput;

  public TopicProcessorFileGenerator(ReplicatedFileGenerator replicatedFileGenerator,
                                     EventHubPojoGenerator eventHubPojoGenerator) {
    this.replicatedFileGenerator = replicatedFileGenerator;
    this.eventHubPojoGenerator = eventHubPojoGenerator;
    this.replicatedClassName = replicatedFileGenerator.getReplicatedImportPath().substring(replicatedFileGenerator.getReplicatedImportPath().lastIndexOf('.')+1);
    this.eventHubClassName = this.replicatedClassName.replace("Repl", "");
    this.processorClassName = this.eventHubClassName + "TopicProcessor";
  }

  public String getGeneratedOutput() {
    return generatedOutput;
  }

  public String getServiceDirectory() {
    return pwd + replSourcePath.substring(1);
  }

  public String getServiceImportPath() {
    return packageName + replicatedClassName.toLowerCase() + "." + processorClassName;
  }

  public String getFullServiceFilePath(String fileName) {
    return getServiceDirectory() + "/" + processorClassName + ".java.txt";
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

  public void addImportStatements(String replicatedImportPath) throws IOException {
    String imports = "import " + replicatedImportPath + ";\n" +
        "import " + eventHubPojoGenerator.getEventHubImportPath() + ";\n" +
        "import com.aa.rac.mod.service.BaseService;\n" +
        "import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;\n" +
        "import java.util.concurrent.ExecutionException;\n" +
        "import org.apache.kafka.clients.consumer.ConsumerRecords;\n" +
        "import org.springframework.beans.factory.annotation.Autowired;\n" +
        "import org.springframework.beans.factory.annotation.Qualifier;\n" +
        "import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;\n" +
        "import org.springframework.context.annotation.Profile;\n" +
        "import org.springframework.kafka.annotation.KafkaListener;\n" +
        "import org.springframework.kafka.support.Acknowledgment;\n" +
        "import org.springframework.stereotype.Component;\n";
    lines.add(imports +"\n");
  }

  public void addClassJavaDoc() {
    lines.add("/** " + eventHubClassName + " topic processor. */\n");
  }

  public String getClassAnnotations() {
    return "@Component\n" +
            "@Profile(\"!JunitTest\")\n" +
            "@ConditionalOnProperty(name = \"spring.kafka.topics.recieve." + eventHubClassName.toLowerCase() + ".enabled\", havingValue = \"true\")";
  }

  public void addInitialClassTemplate(String className) {
    lines.add(getClassAnnotations());
    lines.add("\npublic class " + processorClassName
        + " \n    extends AbstractTopicProcessor {\n");
  }

  public void addFields() {
  }

  public void addConstructor() {
    lines.add("\n  @Autowired\n" +
            "  public "+processorClassName+"(\n" +
            "      @Qualifier(\""+replicatedClassName+"ServiceImpl\")\n" +
            "      BaseService<"+eventHubClassName+", "+ replicatedClassName +"> baseService) {\n" +
            "    super(baseService);\n" +
            "  }\n");
  }

  public void addMethods() {
    lines.add("\n  @KafkaListener(\n" +
            "      id = \"${spring.kafka.topics.recieve."+eventHubClassName.toLowerCase()+".destination}\",\n" +
            "      topics = \"#{'${spring.kafka.topics.recieve."+eventHubClassName.toLowerCase()+".destination}'.split(',')}\",\n" +
            "      concurrency = \"${CONSUMER_CONCURRENCY_COUNT}\",\n" +
            "      clientIdPrefix = \""+eventHubClassName.toLowerCase()+"\",\n" +
            "      batch = \"true\",\n" +
            "      properties = {\n" +
            "        \"auto.offset.reset:earliest\",\n" +
            "        \"reconnect.backoff.max.ms:5000\",\n" +
            "        \"reconnect.backoff.ms:1000\",\n" +
            "        \"retry.backoff.ms:5000\"\n" +
            "      })\n" +
            "  @CircuitBreaker(name = \"${spring.kafka.topics.recieve."+eventHubClassName.toLowerCase()+".destination}\")\n" +
            "  public void "+eventHubClassName.toLowerCase()+"(ConsumerRecords<String, String> payload, Acknowledgment acknowledgment)\n" +
            "      throws InterruptedException, ExecutionException {\n" +
            "    processItems(payload);\n" +
            "    acknowledgment.acknowledge();\n" +
            "  }");
  }

  public void addEndingLine() {
    lines.add("}");
  }

  public void generateConverterFile() throws IOException {
    String processorClassFileName = processorClassName + ".java.txt";
    String fullPath = getFullServiceFilePath(processorClassFileName);
    FileUtil.createFile(getServiceDirectory(), fullPath);
    FileWriter writer = getFileWriter(fullPath);
    addPackageContents(packageName);
    addImportStatements(replicatedFileGenerator.getReplicatedImportPath());
    addClassJavaDoc();
    addInitialClassTemplate(processorClassName);
    addFields();
    addConstructor();
    addMethods();
    addEndingLine();
    this.generatedOutput = String.join("", lines);
//    System.out.println(this.generatedOutput);
    try {
      writer.write(this.generatedOutput);
    } finally {
      writer.close();
    }
    System.out.println("TopicProcessor file successfully generated. Please review at location: " + fullPath);
    System.out.println("\tNOTE: Please review the generated code.");
  }
}
