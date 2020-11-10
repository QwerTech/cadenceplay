package org.qwertech.cadenceplay;

import com.uber.cadence.WorkflowExecution;
import com.uber.cadence.WorkflowIdReusePolicy;
import com.uber.cadence.client.WorkflowClient;
import com.uber.cadence.client.WorkflowClientOptions;
import com.uber.cadence.client.WorkflowOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;

public class HelloAwaitingActivityTest {

  private WorkflowClient workflowClient;
  private final String domain = "test-domain";


  @SneakyThrows
  @Before
  public void setUp() {
    workflowClient = WorkflowClient.newInstance("localhost", 7933, domain,
        new WorkflowClientOptions.Builder().setDataConverter(JacksonDataConverter.getInstance()).build());
  }

  @Test
  @SneakyThrows
  public void testActivityImpl100() {
    List<AtomicInteger> repeats = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      repeats.add(testActivityImpl());
    }
    System.out.println(repeats);
    System.out.println("Avarage: " + repeats.stream().collect(Collectors.averagingInt(AtomicInteger::get)));
  }

//  @Test
  @SneakyThrows
  public AtomicInteger testActivityImpl() {
    List<HelloAwaitingActivity.GreetingWorkflow> wfs = new ArrayList();
    List<WorkflowExecution> wes = new ArrayList();
    // Get a workflow stub using the same task list the worker uses
    final long nanoTime = System.nanoTime();
    for (int i = 0; i < 10; i++) {
      HelloAwaitingActivity.GreetingWorkflow workflow =
          workflowClient
              .newWorkflowStub(HelloAwaitingActivity.GreetingWorkflow.class,
                  new WorkflowOptions.Builder().setWorkflowId(i + "_" + nanoTime)
                      .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.AllowDuplicate).build());
      // Execute a workflow waiting for it to complete.
      WorkflowExecution we = WorkflowClient.start(workflow::getGreeting, i, "World");
      System.out.println("OrderId - " + i + " wid - " + we.workflowId);
      wfs.add(workflow);
      wes.add(we);
    }
    System.out.println("Number of workflows - " + wfs.size());
    AtomicInteger count = new AtomicInteger();
    try {
      wfs.parallelStream()
          .forEach(
              w -> {
                w.noopSignal();
                //              w.unlockSignal();
                System.out.println("query - " + w.queryOrderId());
                count.incrementAndGet();
              });
    } catch (Exception e) {

    }

//    Thread.sleep(20000);
    System.out.println(count + " END!!!");
    return count;
  }
}