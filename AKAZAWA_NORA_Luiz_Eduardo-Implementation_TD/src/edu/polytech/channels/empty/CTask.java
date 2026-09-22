package edu.polytech.channels.empty;

import edu.polytech.channels.Broker;
import edu.polytech.channels.Task;

public class CTask extends Task {

  private final Broker broker;
  protected Runnable boot;
  
  protected boolean alive = false;
  protected boolean dead = false;


  public CTask(Broker b, Runnable r, String name) {
    super(name);
    broker = b;
    boot = r;
    start();
  }

  @Override
  public Broker getBroker() {
    return broker;
  }

  @Override
  public Broker newBroker(String name) {
    return new CBroker(name);
  }

  @Override
  public Task newTask(Broker b, Runnable r, String n) {
    CTask task = new CTask(b, r, n);
    return task;
  }

  @Override
  public boolean alive() { 
	  return alive; 
  }

  @Override
  public boolean dead() { 
	  return dead; 
  }
  
  
  @Override
  public void start() {
    alive = true;
    super.start();
  }

  @Override
  public final void run() {
	try {
    	boot.run();
    } catch (Throwable th) {
    	System.err.println(th.getMessage());
    	th.printStackTrace(System.err);
    } finally {
        alive = false;
        dead = true;
    }
  }

}
