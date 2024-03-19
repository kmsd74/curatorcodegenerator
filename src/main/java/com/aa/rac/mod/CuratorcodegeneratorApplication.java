package com.aa.rac.mod;

import com.aa.rac.mod.codegenerator.AdhoDeleteServiceTestFileGenerator;
import com.aa.rac.mod.codegenerator.AdhocDeleteServiceFileGenerator;
import com.aa.rac.mod.codegenerator.ConverterFileGenerator;
import com.aa.rac.mod.codegenerator.DDLSQLFileGenerator;
import com.aa.rac.mod.codegenerator.FileUtil;
import com.aa.rac.mod.codegenerator.EventHubPojoGenerator;
import com.aa.rac.mod.codegenerator.ReplicatedFileGenerator;
import com.aa.rac.mod.codegenerator.RepositoryFileGenerator;
import com.aa.rac.mod.codegenerator.ServiceFileGenerator;
import com.aa.rac.mod.codegenerator.TestFileGenerator;
import com.aa.rac.mod.codegenerator.TopicProcessorFileGenerator;
import com.aa.rac.mod.codegenerator.ValidatorYamlFileGenerator;

import java.io.IOException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.convert.converter.Converter;

@SpringBootApplication
public class CuratorcodegeneratorApplication {
	public static final String SCHEMA_NAME = "replicated";

	public static void main(String[] args) throws IOException {
		SpringApplication.run(CuratorcodegeneratorApplication.class, args);

		String fileName = "agdeitin";
		String[] uuids = {"COUPON_UUID","TICKET_UUID"};
		String tableName = "agyd0001_agyt0110_agdeitin";
		boolean filterDefaultTicketCreateTs = true;
		boolean adhocDeleteEquivalent = true;

		String resourcesPath = System.getProperty("user.dir").replace('\\', '/')
				+ "/src/main/resources/" + fileName +"/";

		String insertFilePath = resourcesPath + fileName + "_insert.txt";
		String updateFilePath = resourcesPath + fileName + ".txt";
		String deleteFilePath = resourcesPath + fileName + "_delete.txt";


		String ddlFilePath = resourcesPath + fileName + "_ddl.txt";

		
		ValidatorYamlFileGenerator validatorYamlFileGenerator = new ValidatorYamlFileGenerator(fileName,ddlFilePath, tableName);
		validatorYamlFileGenerator.generateValidatorYamlFile();


		// DDLSQLFileGenerator ddlsqlFileGenerator = new DDLSQLFileGenerator(ddlFilePath, tableName, uuids);
		// ddlsqlFileGenerator.generateDDLFile();
		// System.out.println("\n");

		// EventHubPojoGenerator eventHubPojoGenerator = new EventHubPojoGenerator(updateFilePath, ddlsqlFileGenerator, filterDefaultTicketCreateTs);
		// eventHubPojoGenerator.eventHubPojoFileGenerator();
		// System.out.println("\n");

		// ReplicatedFileGenerator replicatedFileGenerator = new ReplicatedFileGenerator(updateFilePath, ddlsqlFileGenerator);
		// replicatedFileGenerator.generateReplicatedFile();
		// System.out.println("\n");

		// String replImportPath = replicatedFileGenerator.getReplicatedImportPath();
		// String replClassName = replImportPath.substring(replImportPath.lastIndexOf(".")+1);

		// RepositoryFileGenerator repositoryFileGenerator = new RepositoryFileGenerator(ddlsqlFileGenerator, replClassName);
		// repositoryFileGenerator.generateRepositoryFile(replImportPath);


		// System.out.println("\n");
		// ConverterFileGenerator converterFileGenerator = new ConverterFileGenerator(replicatedFileGenerator, eventHubPojoGenerator, ddlsqlFileGenerator);
		// converterFileGenerator.generateConverterFile();

		// System.out.println("\n");
		// ServiceFileGenerator serviceFileGenerator = new ServiceFileGenerator(replicatedFileGenerator, eventHubPojoGenerator, repositoryFileGenerator, filterDefaultTicketCreateTs);
		// serviceFileGenerator.generateConverterFile();


		// System.out.println("\n");
		// TopicProcessorFileGenerator topicProcessorFileGenerator = new TopicProcessorFileGenerator(replicatedFileGenerator, eventHubPojoGenerator);
		// topicProcessorFileGenerator.generateConverterFile();

		// System.out.println("\n");
		// TestFileGenerator testFileGenerator = new TestFileGenerator(serviceFileGenerator, ddlsqlFileGenerator, insertFilePath, updateFilePath, deleteFilePath);
		// testFileGenerator.generateConverterFile();

		// if (adhocDeleteEquivalent) {
		// 	System.out.println("\n");
		// 	AdhocDeleteServiceFileGenerator adhocDeleteServiceFileGenerator = new AdhocDeleteServiceFileGenerator(replicatedFileGenerator, eventHubPojoGenerator, repositoryFileGenerator);
		// 	adhocDeleteServiceFileGenerator.generateConverterFile();

		// 	System.out.println("\n");
		// 	AdhoDeleteServiceTestFileGenerator adhoDeleteServiceTestFileGenerator = new AdhoDeleteServiceTestFileGenerator(adhocDeleteServiceFileGenerator, ddlsqlFileGenerator, insertFilePath, updateFilePath, deleteFilePath);
		// 	adhoDeleteServiceTestFileGenerator.generateConverterFile();
		// }
	}

}
