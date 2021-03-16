package org.qwertech.cadenceplay;

import com.uber.cadence.activity.ActivityMethod;
import com.uber.cadence.client.WorkflowClient;
import com.uber.cadence.client.WorkflowOptions;
import com.uber.cadence.client.WorkflowOptions.Builder;
import com.uber.cadence.worker.Worker;
import com.uber.cadence.workflow.QueryMethod;
import com.uber.cadence.workflow.SignalMethod;
import com.uber.cadence.workflow.Workflow;
import com.uber.cadence.workflow.WorkflowMethod;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

@Slf4j
public class CadenceExceptions {

  public static final String TEST_DOMAIN = "test-domain";
  public static final String TASK_LIST = "HelloWorldTaskList";
  private static final WorkflowOptions WORKFLOW_OPTIONS = new Builder().setTaskList(TASK_LIST).build();
  private static final Logger wfLog = Workflow.getLogger(CadenceExceptions.class);

  @SneakyThrows
  public static void main(String[] args) {

    Worker.Factory factory = new Worker.Factory(TEST_DOMAIN);
    Utils.createDomainIfNotExists(factory, TEST_DOMAIN);
    Worker worker = factory.newWorker(TASK_LIST);
    worker.registerWorkflowImplementationTypes(LongWorkflowImpl.class);
    WorkflowClient workflowClient = WorkflowClient.newInstance(TEST_DOMAIN);
    worker.registerActivitiesImplementations(new ConnectorActivityImpl());
    factory.start();
    String longWorkflowId = "someVeryLongWorkflowId_" + UUID.randomUUID();
    log.info("Started {}", longWorkflowId);

    WorkflowOptions options = new Builder(WORKFLOW_OPTIONS).setWorkflowId(longWorkflowId).build();
    LongWorkflow realWorkflow = workflowClient.newWorkflowStub(LongWorkflow.class, options);

    WorkflowClient.start(realWorkflow::startLongProcess);
    LongWorkflow stub = workflowClient.newWorkflowStub(LongWorkflow.class, longWorkflowId);
    stub.update("123");
    stub.getState();
    System.out.println("Bye!");
  }

  public interface ConnectorActivity {

    @ActivityMethod(scheduleToCloseTimeoutSeconds = 3)
    SomeState getState(String longWorkflowId);

    @ActivityMethod(scheduleToCloseTimeoutSeconds = 3)
    void updateState(String longWorkflowId, String someNewData);
  }

  public interface LongWorkflow {

    @WorkflowMethod(executionStartToCloseTimeoutSeconds = 60)
    String startLongProcess();

    @SignalMethod
    void update(String someNewData);

    @QueryMethod
    SomeState getState();
  }

  public static class LongWorkflowImpl implements LongWorkflow {

    private final ConnectorActivity activities = Workflow.newActivityStub(ConnectorActivity.class);
    private final SomeState state = new SomeState("someInitialData");

    @Override
    public String startLongProcess() {
      System.out.println("I'm long workflow and I'm started");
      Workflow.await(() -> state.getSomeData().equals("ByeData"));
      System.out.println("I'm long workflow and I'm done");
      return state.getSomeData();
    }

    @Override
    public void update(String someNewData) {
      activities.updateState("asdd", "some");
      System.out.println(String.format("I'm long workflow and I've got new data %s", someNewData));
      state.setSomeData(someNewData);
    }

    @Override
    public SomeState getState() {
      return state;
    }
  }

  @AllArgsConstructor
  public static class ConnectorActivityImpl implements ConnectorActivity {

    @Override
    public SomeState getState(String longWorkflowId) {
      return null;
    }

    @Override
    public void updateState(String longWorkflowId, String someNewData) {
      throw new IllegalStateException();
    }
  }

  @Data
  @AllArgsConstructor
  public static class SomeState {

    private String someData;
  }
}
