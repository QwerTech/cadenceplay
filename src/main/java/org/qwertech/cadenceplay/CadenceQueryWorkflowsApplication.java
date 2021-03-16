package org.qwertech.cadenceplay;

import static java.util.stream.Collectors.toSet;

import com.uber.cadence.ListOpenWorkflowExecutionsRequest;
import com.uber.cadence.ListOpenWorkflowExecutionsResponse;
import com.uber.cadence.StartTimeFilter;
import com.uber.cadence.WorkflowTypeFilter;
import com.uber.cadence.serviceclient.IWorkflowService;
import com.uber.cadence.serviceclient.WorkflowServiceTChannel;
import com.uber.cadence.serviceclient.WorkflowServiceTChannel.ClientOptions;
import com.uber.cadence.serviceclient.WorkflowServiceTChannel.ClientOptions.Builder;
import java.util.Set;
import lombok.SneakyThrows;

public class CadenceQueryWorkflowsApplication {

  public static final String DOMAIN = "test-domain";
  public static final String CADENCE_HOST = "localhost";
  public static final int CADENCE_PORT = 7933;
  public static final String WF_NAME = "GeneratedWorkflow::process";
  public static final int RPC_TIMEOUT = 60_000;
  public static final int MAXIMUM_PAGE_SIZE = 10_000;

  public static void main(String[] args) {
    ClientOptions clientOptions = new Builder()
        .setRpcTimeout(RPC_TIMEOUT)
        .build();

    IWorkflowService wfService = new WorkflowServiceTChannel(CADENCE_HOST, CADENCE_PORT, clientOptions);
    try {
      Set<String> wfIds = getOpenedWfIds(wfService, DOMAIN, WF_NAME);
      System.out.println(wfIds);
      System.out.println(wfIds.size());
    } finally {
      wfService.close();
    }

  }

  @SneakyThrows
  private static Set<String> getOpenedWfIds(IWorkflowService wfService, String domain, String wfName) {
    StartTimeFilter startTimeFilter = new StartTimeFilter()
        .setEarliestTime(0)
        .setLatestTime(Long.MAX_VALUE);
    ListOpenWorkflowExecutionsRequest request = new ListOpenWorkflowExecutionsRequest()
        .setDomain(domain)
        .setMaximumPageSize(
            MAXIMUM_PAGE_SIZE)
        .setTypeFilter(new WorkflowTypeFilter().setName(wfName))
        .setStartTimeFilter(startTimeFilter);

    ListOpenWorkflowExecutionsResponse response = wfService.ListOpenWorkflowExecutions(request);
    Set<String> result = map(response);

    while (response.getExecutions().size() == MAXIMUM_PAGE_SIZE) {
      response = wfService.ListOpenWorkflowExecutions(request.setNextPageToken(response.getNextPageToken()));
      result.addAll(map(response));
    }
    return result;
  }

  private static Set<String> map(ListOpenWorkflowExecutionsResponse response) {
    return response.getExecutions().stream().map(e -> e.getExecution().getWorkflowId()).collect(toSet());
  }

}
