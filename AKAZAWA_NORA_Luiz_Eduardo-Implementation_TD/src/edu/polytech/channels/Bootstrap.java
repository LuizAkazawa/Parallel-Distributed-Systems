package edu.polytech.channels;

public interface Bootstrap {
    Broker newBroker(String name);
    Task newTask(Broker b, Runnable r, String name);
}
