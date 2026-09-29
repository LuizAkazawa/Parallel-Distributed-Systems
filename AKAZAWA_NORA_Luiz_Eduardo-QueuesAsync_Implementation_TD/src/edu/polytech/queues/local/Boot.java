package edu.polytech.queues.local;

import edu.polytech.queues.Bootstrap;
import edu.polytech.queues.Task;
import edu.polytech.utils.Executor;

public class Boot implements Bootstrap {

    public Boot() {
        new BrokerManager();
    }

    @Override
    public Task newTask(Runnable r, String name) {
        Task t = Executor.self().newTask(name);
        t.post(r);
        return t;
    }
}
