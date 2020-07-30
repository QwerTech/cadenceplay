package org.qwertech.cadenceplay;

import com.uber.cadence.client.WorkflowClient;
import com.uber.cadence.client.WorkflowClientOptions;
import com.uber.cadence.client.WorkflowOptions;
import com.uber.cadence.client.WorkflowOptions.Builder;
import com.uber.cadence.client.WorkflowStub;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import lombok.Data;
import org.apache.thrift.TException;

public class ExecuteWorkflowSimple {

  public static void main(String[] args) throws IOException {

    final Properties properties = new Properties();
    properties.load(new FileInputStream("/config.properties"));

    WorkflowClient workflowClient = WorkflowClient.newInstance("localhost", 7933, properties.getProperty("domain"),
        new WorkflowClientOptions.Builder().setDataConverter(JacksonDataConverter.getInstance()).build());

    final String workflowId = UUID.randomUUID().toString();
    System.out.println("Starting workflow with id " + workflowId);
    WorkflowOptions options = new Builder()
        .setTaskList(properties.getProperty("task_list"))
        .setExecutionStartToCloseTimeout(Duration.ofHours(1))
        .setWorkflowId(workflowId).build();

    final OsagoSubjectObjectCheckRequest input = new OsagoSubjectObjectCheckRequest();

    final WorkflowStub workflowStub = workflowClient.newUntypedWorkflowStub(properties.getProperty("workflow_type"), options);
    workflowStub.start(input);
    workflowStub.getResult(OsagoSubjectObjectCheckResponse.class);

  }

  @Data
  public static class OsagoSubjectObjectCheckRequest implements Serializable {

    private static final long serialVersionUID = 1L;
    private String insurerId = null;
    private String initiatorId = null;
    private String requestId = null;
    //    private Party party = null;
//    private Vehicle vehicle = null;
    private OffsetDateTime actualizationDate = null;
  }

  @Data
  public static class OsagoSubjectObjectCheckResponse implements Serializable {

    private static final long serialVersionUID = 1L;
    private String insurerId = null;
    private String requestId = null;
    private FlkReport validationReport = null;
    //    private SubjectObjectCheckFlag mdmFoundIndicator = null;
//    private List<DocumentCheckStatus> documentCheckStatuses = null;
    private Boolean successIndicator = null;
  }

  @Data
  public static class FlkReport implements Serializable {

    private static final long serialVersionUID = 1L;
    private UUID requestId = null;
    private String businessObjectId = null;
    private List<Mismatch> mismatches = new ArrayList();
  }

  @Data
  public static class Mismatch implements Serializable {

    private static final long serialVersionUID = 1L;
    private String code = null;
    private String message = null;
    private Boolean isCritical = null;
    private String codeExceptional = null;
    private String externalObjectId = null;
    private String businessObjectId = null;
//    private List<MismatchElement> mismatchElements = null;
  }
}
