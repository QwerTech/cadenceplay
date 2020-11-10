package org.qwertech.cadenceplay;

import com.uber.cadence.DomainAlreadyExistsError;
import com.uber.cadence.RegisterDomainRequest;
import com.uber.cadence.serviceclient.IWorkflowService;
import com.uber.cadence.serviceclient.WorkflowServiceTChannel;
import lombok.experimental.UtilityClass;
import org.apache.thrift.TException;

@UtilityClass
public class Utils {

  public static IWorkflowService createDomain(String domain) throws TException {

    IWorkflowService cadenceService = new WorkflowServiceTChannel();
    RegisterDomainRequest request = new RegisterDomainRequest();
    request.setDescription("sample domain");
    request.setEmitMetric(false);
    request.setName(domain);
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
}
