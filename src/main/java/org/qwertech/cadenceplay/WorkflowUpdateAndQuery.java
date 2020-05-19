package org.qwertech.cadenceplay;

import com.uber.cadence.DomainAlreadyExistsError;
import com.uber.cadence.RegisterDomainRequest;
import com.uber.cadence.activity.ActivityMethod;
import com.uber.cadence.client.WorkflowClient;
import com.uber.cadence.client.WorkflowOptions;
import com.uber.cadence.client.WorkflowOptions.Builder;
import com.uber.cadence.serviceclient.IWorkflowService;
import com.uber.cadence.serviceclient.WorkflowServiceTChannel;
import com.uber.cadence.worker.Worker;
import com.uber.cadence.workflow.QueryMethod;
import com.uber.cadence.workflow.SignalMethod;
import com.uber.cadence.workflow.Workflow;
import com.uber.cadence.workflow.WorkflowMethod;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.thrift.TException;
import org.slf4j.Logger;

public class WorkflowUpdateAndQuery {

  public static final String TEST_DOMAIN = "test-domain";
  public static final String TASK_LIST = "HelloWorldTaskList";
  private static final WorkflowOptions WORKFLOW_OPTIONS = new Builder().setTaskList(TASK_LIST).build();
  private static Logger logger = Workflow.getLogger(WorkflowUpdateAndQuery.class);

  public static IWorkflowService domain() throws TException {

    IWorkflowService cadenceService = new WorkflowServiceTChannel();
    RegisterDomainRequest request = new RegisterDomainRequest();
    request.setDescription("sample domain");
    request.setEmitMetric(false);
    request.setName(TEST_DOMAIN);
    int retentionPeriodInDays = 5;
    request.setWorkflowExecutionRetentionPeriodInDays(retentionPeriodInDays);
    try {
      cadenceService.RegisterDomain(request);
//      logger.debug("Successfully registered domain {} with retentionDays={}", logger,
//          retentionPeriodInDays);
    } catch (DomainAlreadyExistsError e) {
//      logger.warn("domain  already exists {} {}", TEST_DOMAIN, e);
    }
    return cadenceService;
  }

  public static void main(String[] args) throws TException {

    domain();
    Worker.Factory factory = new Worker.Factory(TEST_DOMAIN);
    Worker worker = factory.newWorker(TASK_LIST);
    worker.registerWorkflowImplementationTypes(LongWorkflowImpl.class, ShortWorkflowImpl.class);
    WorkflowClient workflowClient = WorkflowClient.newInstance(TEST_DOMAIN);
    worker.registerActivitiesImplementations(new ConnectorActivityImpl(workflowClient));
    factory.start();
    String longWorkflowId = "someVeryLongWorkflowId_"+ UUID.randomUUID();

    new Thread(() -> runShortWorkflow(workflowClient, longWorkflowId)).start();

//    new Thread(() -> {
//      runLongWorkflowSignal(workflowClient, longWorkflowId);
//    });

    runLongWorkflowSignal(workflowClient, longWorkflowId);
    System.out.println("Bye!");
  }

  private static void runLongWorkflowSignal(WorkflowClient workflowClient, String longWorkflowId) {
    WorkflowOptions options = new Builder(WORKFLOW_OPTIONS).setWorkflowId(longWorkflowId).build();
    workflowClient.newWorkflowStub(LongWorkflow.class, options).startLongProcess();
  }

  @SneakyThrows
  private static void runShortWorkflow(WorkflowClient workflowClient, String longWorkflowId) {
    Thread.sleep(3_000);
    ShortWorkflow shortWorkflow = workflowClient.newWorkflowStub(ShortWorkflow.class, WORKFLOW_OPTIONS);
    SomeState someStateFromLongWorkflow = shortWorkflow.getStateFromLong(longWorkflowId);
    Thread.sleep(3_000);
    shortWorkflow = workflowClient.newWorkflowStub(ShortWorkflow.class, WORKFLOW_OPTIONS);
    someStateFromLongWorkflow = shortWorkflow.getStateFromLong(longWorkflowId);
    Thread.sleep(3_000);
    workflowClient.newWorkflowStub(LongWorkflow.class, longWorkflowId).update("someNewData");
    Thread.sleep(3_000);
    shortWorkflow = workflowClient.newWorkflowStub(ShortWorkflow.class, WORKFLOW_OPTIONS);
    someStateFromLongWorkflow = shortWorkflow.getStateFromLong(longWorkflowId);
    Thread.sleep(3_000);
    workflowClient.newWorkflowStub(LongWorkflow.class, longWorkflowId).update("ByeData");
    Thread.sleep(3_000);
    shortWorkflow = workflowClient.newWorkflowStub(ShortWorkflow.class, WORKFLOW_OPTIONS);
    someStateFromLongWorkflow = shortWorkflow.getStateFromLong(longWorkflowId);


  }

  public interface LongWorkflow {

    @WorkflowMethod(executionStartToCloseTimeoutSeconds = 60)
    void startLongProcess();

    @SignalMethod
    void update(String someNewData);

    @QueryMethod
    SomeState getState();
  }

  public static class LongWorkflowImpl implements LongWorkflow {

    private final GettingStartedActivities.HelloWorldActivities activities = Workflow.newActivityStub(GettingStartedActivities.HelloWorldActivities.class);
    private final SomeState state = new SomeState("someInitialData");

    @Override
    public void startLongProcess() {
      System.out.println("I'm long workflow and I'm started");
      Workflow.await(() -> state.getSomeData().equals("ByeData"));
      System.out.println("I'm long workflow and I'm done");
    }

    @Override
    public void update(String someNewData) {
      System.out.println(String.format("I'm long workflow and I've got new data %s", someNewData));
      state.setSomeData(someNewData);
    }

    @Override
    public SomeState getState() {
      System.out.println(String.format("I'm long workflow and I'm returning data %s", state.getSomeData()));
      return state;
    }
  }

  public interface ShortWorkflow {

    @WorkflowMethod(executionStartToCloseTimeoutSeconds = 3)
    SomeState getStateFromLong(String longWorkflowId);
  }

  public interface ConnectorActivity {

    @ActivityMethod(scheduleToCloseTimeoutSeconds = 3)
    SomeState getState(String longWorkflowId);
    @ActivityMethod(scheduleToCloseTimeoutSeconds = 3)
    void updateState(String longWorkflowId, String someNewData);
  }

  @AllArgsConstructor
  public static class ConnectorActivityImpl implements ConnectorActivity {

    private WorkflowClient workflowClient;

    @Override
    public SomeState getState(String longWorkflowId) {
      LongWorkflow longWorkflow = workflowClient.newWorkflowStub(LongWorkflow.class, longWorkflowId);
      SomeState state = longWorkflow.getState();
      System.out.println(String.format("I'm connector activity and I've got %s from long workflow", state.getSomeData()));
      return state;
    }

    @Override
    public void updateState(String longWorkflowId, String someNewData) {
      workflowClient.newWorkflowStub(LongWorkflow.class, longWorkflowId).update(someNewData);
    }
  }

  @Data
  @AllArgsConstructor
  public static class SomeState {

    private String someData;
  }

  public static class ShortWorkflowImpl implements ShortWorkflow {

    private final ConnectorActivity connectorActivity = Workflow.newActivityStub(ConnectorActivity.class);

    @Override
    public SomeState getStateFromLong(String longWorkflowId) {
      connectorActivity.updateState(longWorkflowId, "123");
      Workflow.await(() -> connectorActivity.getState(longWorkflowId).getSomeData().equals("123"));
      SomeState state = connectorActivity.getState(longWorkflowId);
      System.out.println(String.format("I'm short workflow and I've got %s from long workflow thought connector activity", state.getSomeData()));
      return state;
    }

  }


}
