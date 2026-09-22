package edu.polytech.channels;

public abstract class Task extends Thread {
    public Task(Broker b, Runnable r) {
    }
    
    public Task(String name) {
        super(name);
    }

    public static Task task() {
    	Thread current = Thread.currentThread();
    	if (current instanceof Task) {
    		return (Task) current;
    	}
    	throw new IllegalStateException("Current thread is not a Task");
    }
    
    public abstract boolean alive();
    
    public abstract boolean dead();

    public abstract Broker getBroker();
    
    public abstract Broker newBroker(String name);
    
    public abstract Task newTask(Broker b, Runnable r, String name);
}
