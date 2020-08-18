/*
 * VTB Group. Do not reproduce without permission in writing.
 * Copyright (c) 2020 VTB Group. All rights reserved.
 */

package org.qwertech.cadenceplay;

import com.uber.cadence.DomainAlreadyExistsError;
import com.uber.cadence.RegisterDomainRequest;
import com.uber.cadence.activity.ActivityMethod;
import com.uber.cadence.internal.worker.PollerOptions.Builder;
import com.uber.cadence.serviceclient.IWorkflowService;
import com.uber.cadence.serviceclient.WorkflowServiceTChannel;
import com.uber.cadence.worker.Worker;
import com.uber.cadence.worker.Worker.Factory;
import com.uber.cadence.workflow.QueryMethod;
import com.uber.cadence.workflow.SignalMethod;
import com.uber.cadence.workflow.Workflow;
import com.uber.cadence.workflow.WorkflowMethod;
import lombok.SneakyThrows;

public class HelloAwaitingActivity {

  private static String domain;
  private static Worker worker;

  public static void main(String[] args) {
    domain = "test-domain";
    domain();

    Worker.FactoryOptions.Builder foBuilder = new Worker.FactoryOptions.Builder()
        .setMaxWorkflowThreadCount(5)
        .setStickyWorkflowPollerOptions(new Builder().setPollThreadCount(1).build());

    final Factory factory = new Factory(domain, foBuilder.build());
    worker = factory.newWorker(HelloAwaitingActivity.TASK_LIST);
    worker.registerWorkflowImplementationTypes(HelloAwaitingActivity.GreetingWorkflowImpl.class);
    worker.registerActivitiesImplementations(new HelloAwaitingActivity.GreetingActivitiesImpl());
    factory.start();
  }

  @SneakyThrows
  public static IWorkflowService domain() {

    IWorkflowService cadenceService = new WorkflowServiceTChannel();
    RegisterDomainRequest request = new RegisterDomainRequest();
    request.setDescription("sample domain");
//    request.setEmitMetric(false);
    request.setName(domain);
    int retentionPeriodInDays = 5;
    request.setWorkflowExecutionRetentionPeriodInDays(retentionPeriodInDays);
    try {
      cadenceService.RegisterDomain(request);
    } catch (DomainAlreadyExistsError e) {
    }
    return cadenceService;
  }

  public static final String TASK_LIST = "HelloAwaitingActivity";

  /**
   * Workflow interface has to have at least one method annotated with @WorkflowMethod.
   */
  public interface GreetingWorkflow {

    /**
     * @return greeting string
     */
    @WorkflowMethod(executionStartToCloseTimeoutSeconds = 6000, taskList = TASK_LIST)
    String getGreeting(int orderId, String name);

    @SignalMethod
    void noopSignal();

    @SignalMethod
    void unlockSignal();

    @QueryMethod
    int queryOrderId();
  }

  /**
   * Activity interface is just a POJI.
   */
  public interface GreetingActivities {

    @ActivityMethod(scheduleToCloseTimeoutSeconds = 600)
    String composeGreeting(String greeting, String name);
  }

  /**
   * GreetingWorkflow implementation that calls GreetingsActivities#composeGreeting.
   */
  public static class GreetingWorkflowImpl implements GreetingWorkflow {

    private boolean unlock = false;
    private int id;

    /**
     * Activity stub implements activity interface and proxies calls to it to Cadence activity invocations. Because activities are reentrant, only a single stub
     * can be used for multiple activity invocations.
     */
    private final GreetingActivities activities =
        Workflow.newActivityStub(GreetingActivities.class);

    @Override
    public String getGreeting(int orderId, String name) {
      this.id = orderId;
      System.out.println("Start getGreeting - " + orderId);
      // This is a blocking call that returns only after the activity has completed.
      System.out.println("LOCK  order - " + orderId);
      Workflow.await(() -> unlock);
      System.out.println("UNLOCK order - " + orderId);
      String res = activities.composeGreeting("Hello", name);
      System.out.println("Stop getGreeting - " + orderId);
      return res;
    }

    @Override
    public void noopSignal() {
      System.out.println("noopSignal - " + id);
    }

    @Override
    public void unlockSignal() {
      System.out.println("unlockSignal - " + id);
      unlock = true;
    }

    @Override
    public int queryOrderId() {
      System.out.println("queryOrderId - " + id);
      return id;
    }
  }

  public static class GreetingActivitiesImpl implements GreetingActivities {

    @Override
    public String composeGreeting(String greeting, String name) {
      return greeting + " " + name + "!";
    }
  }
}