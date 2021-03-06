package org.qwertech.cadenceplay;

import com.uber.cadence.worker.Worker;
import com.uber.cadence.workflow.QueryMethod;
import com.uber.cadence.workflow.SignalMethod;
import com.uber.cadence.workflow.Workflow;
import com.uber.cadence.workflow.WorkflowMethod;
import org.slf4j.Logger;

import java.util.Objects;

public class GettingStarted {
    private static Logger logger = Workflow.getLogger(GettingStarted.class);

    public static void main(String[] args) {
        Worker.Factory factory = new Worker.Factory("test-domain");
        Worker worker = factory.newWorker("HelloWorldTaskList");
        worker.registerWorkflowImplementationTypes(HelloWorldImpl.class);
        factory.start();
    }

    public interface HelloWorld {
        @WorkflowMethod
        void sayHello(String name);

        @SignalMethod
        void updateGreeting(String greeting);

        @QueryMethod
        int getCount();
    }

    public static class HelloWorldImpl implements HelloWorld {
        private final GettingStartedActivities.HelloWorldActivities activities = Workflow.newActivityStub(GettingStartedActivities.HelloWorldActivities.class);
        private String greeting = "Hello";
        private int count = 0;

        @Override
        public void sayHello(String name) {
            while (!"Bye".equals(greeting)) {
                activities.say(++count + ": " + greeting + " " + name + "!");
                String oldGreeting = greeting;
                Workflow.await(() -> !Objects.equals(greeting, oldGreeting));
            }
            activities.say(++count + ": " + greeting + " " + name + "!");
        }

        @Override
        public void updateGreeting(String greeting) {
            this.greeting = greeting;
        }

        @Override
        public int getCount() {
            return count;
        }
    }
}
