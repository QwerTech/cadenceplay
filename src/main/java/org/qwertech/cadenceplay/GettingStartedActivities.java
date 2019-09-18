package org.qwertech.cadenceplay;

import com.uber.cadence.activity.ActivityMethod;
import com.uber.cadence.worker.Worker;
import com.uber.cadence.workflow.QueryMethod;
import com.uber.cadence.workflow.SignalMethod;
import com.uber.cadence.workflow.Workflow;
import com.uber.cadence.workflow.WorkflowMethod;
import org.slf4j.Logger;

import java.io.PrintStream;
import java.util.Objects;

public class GettingStartedActivities {
    private static Logger logger = Workflow.getLogger(GettingStartedActivities.class);
    public static void main(String[] args) {
        Worker.Factory factory = new Worker.Factory("test-domain");
        Worker worker = factory.newWorker("HelloWorldTaskList");
        worker.registerActivitiesImplementations(new HelloWordActivitiesImpl(System.out));
        factory.start();
    }
    public interface HelloWorldActivities {
        @ActivityMethod(scheduleToCloseTimeoutSeconds = 100)
        void say(String message);
    }
    public static class HelloWordActivitiesImpl implements HelloWorldActivities {
        private final PrintStream out;

        public HelloWordActivitiesImpl(PrintStream out) {
            this.out = out;
        }

        @Override
        public void say(String message) {
            out.println(message);
        }
    }
}
