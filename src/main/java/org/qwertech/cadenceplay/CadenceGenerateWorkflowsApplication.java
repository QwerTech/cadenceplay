package org.qwertech.cadenceplay;

import com.uber.cadence.client.WorkflowClient;
import com.uber.cadence.client.WorkflowOptions;
import com.uber.cadence.client.WorkflowOptions.Builder;
import com.uber.cadence.serviceclient.IWorkflowService;
import com.uber.cadence.serviceclient.WorkflowServiceTChannel;
import com.uber.cadence.workflow.WorkflowMethod;
import java.util.stream.IntStream;

public class CadenceGenerateWorkflowsApplication {

  public static final String TASK_LIST = "GeneratedWorkflowsTaskList";
  public static final String DOMAIN = "test-domain";
  public static final int WORKFLOWS_COUNT = 100_000;

  public static void main(String[] args) {

    IWorkflowService workflowService = new WorkflowServiceTChannel();
    Utils.createDomainIfNotExists(workflowService, DOMAIN);
    WorkflowClient workflowClient = WorkflowClient.newInstance(DOMAIN);
    try {
      WorkflowOptions options = new Builder(new Builder().setTaskList(TASK_LIST).build()).build();
      IntStream.range(0, WORKFLOWS_COUNT).forEach(i -> startWf(workflowClient, options));
    } finally {
      workflowClient.close();
      workflowService.close();
    }
  }

  private static void startWf(WorkflowClient workflowClient, WorkflowOptions options) {
    GeneratedWorkflow wfStub = workflowClient.newWorkflowStub(GeneratedWorkflow.class, options);
    WorkflowClient.start(wfStub::process);
  }


  public interface GeneratedWorkflow {

    //5 days
    @WorkflowMethod(executionStartToCloseTimeoutSeconds = 5 * 24 * 60 * 60)
    String process();
  }
}
