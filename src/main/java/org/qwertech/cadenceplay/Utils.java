package org.qwertech.cadenceplay;

import com.uber.cadence.DomainAlreadyExistsError;
import com.uber.cadence.RegisterDomainRequest;
import com.uber.cadence.serviceclient.IWorkflowService;
import com.uber.cadence.worker.Worker;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

@UtilityClass
public class Utils {

  public static void createDomainIfNotExists(Worker.Factory factory, String domain) {
    createDomainIfNotExists(factory.getWorkflowService(), domain);
  }

  @SneakyThrows
  public static void createDomainIfNotExists(IWorkflowService workflowService, String domain) {

    RegisterDomainRequest request = new RegisterDomainRequest()
        .setDescription("sample domain")
        .setEmitMetric(false)
        .setName(domain)
        .setWorkflowExecutionRetentionPeriodInDays(5);
    try {
      workflowService.RegisterDomain(request);
    } catch (DomainAlreadyExistsError ignored) {
    }
  }
}
