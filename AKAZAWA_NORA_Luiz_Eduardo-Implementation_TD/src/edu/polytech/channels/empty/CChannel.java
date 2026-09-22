package edu.polytech.channels.empty;

import edu.polytech.channels.Channel;
import edu.polytech.utils.CircularBuffer;

public class CChannel implements Channel {
	
	private final CircularBuffer tx;
	private final CircularBuffer rx;
	private boolean disconnected = false;
	

	CChannel(CircularBuffer tx, CircularBuffer rx) {
        this.tx = tx;
        this.rx = rx;
    }

	@Override
  	public int read(byte[] bytes, int offset, int length) {
	  int readCount = 0;
	  
	  synchronized(rx) {
		  while(rx.empty() && !disconnected) {
			  try {
				rx.wait();
			  } catch (InterruptedException e) {
				e.printStackTrace();
			  }
		  }
		  
		  if (disconnected && rx.empty()) { return 0; }
		  
		  while(readCount < length && !rx.empty()) {
			  bytes[offset + readCount] = rx.pull();
			  readCount++;
		  }
		  rx.notifyAll();
	  }
	  
	  return readCount;
  	}

  	@Override
  	public int write(byte[] bytes, int offset, int length) {
	  if(disconnected) {return 0;}
	  
	  int written = 0;
	  synchronized(tx) {
		  while(tx.full() && !disconnected) {
			  try {
				tx.wait();
			  } catch (InterruptedException e) {
				e.printStackTrace();
			  }
		  }
		  if(disconnected) {return 0;}
		  
		  while(written < length && !tx.full()) {
			  tx.push(bytes[offset + written]);
			  written++;
		  }
		  tx.notifyAll();
	  }
	  
	  return written;
	  
  	}

  	@Override
  	public boolean disconnected() {
	  return disconnected;
  	}

  	@Override
  	public void disconnect() {
	  disconnected = true;
	  synchronized (rx) {
	        rx.notifyAll();
	    }
	  synchronized (tx) {
	        tx.notifyAll();
	    }
  	}

}
