package edu.polytech.channels.empty;

import java.util.concurrent.ConcurrentHashMap;

import edu.polytech.channels.Broker;
import edu.polytech.channels.Channel;
import edu.polytech.utils.CircularBuffer;

public class CBroker implements Broker {
	
	private final String name;
	private final ConcurrentHashMap<Integer, PortState> portsMap = new ConcurrentHashMap<>();

	CBroker(String name) {
		this.name = name;
		BrokerManager.getInstance().add(this);
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public Channel connect(String name, int port) {
		CBroker remoteBroker = BrokerManager.getInstance().get(name);
		if(remoteBroker == null) {
			return null;
		}
		
		remoteBroker.portsMap.putIfAbsent(port, remoteBroker.new PortState());
	    PortState state = remoteBroker.portsMap.get(port);
	    ConnectRequest req = null;
	    
	    synchronized(state) {
	    	if(state.isAccepting) {
	    		CircularBuffer buf1 = new CircularBuffer(64);
			    CircularBuffer buf2 = new CircularBuffer(64);
			    
			    CChannel localChannel = new CChannel(buf1, buf2);
			    CChannel remoteChannel = new CChannel(buf2, buf1);
			    
			    state.isAccepting = false;
			    state.channel = remoteChannel;
			    state.notify();
			    return localChannel;
	    	}else {
	    		req = remoteBroker.new ConnectRequest();
	    		state.connectQueue.add(req);
	    		
	    	}
	    }
	    
	    synchronized (req) {
	        while (req.channel == null) {
	            try {
	                req.wait();
	            } catch (InterruptedException e) {
	                e.printStackTrace();
	            }
	        }
	        return req.channel;
	    }
	    
	}

	@Override
	public Channel accept(int port) {
		portsMap.putIfAbsent(port, new PortState());
		PortState state = portsMap.get(port);
		
		synchronized(state) {
		    if (state.isAccepting) {
		        throw new IllegalStateException("Port is already accepting");
		    }
			
			if(state.connectQueue.isEmpty()) {
				state.isAccepting = true;
				try {
					while(state.isAccepting) {
						state.wait();	
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}else {
				ConnectRequest req = state.connectQueue.poll();
				
				CircularBuffer buf1 = new CircularBuffer(64);
			    CircularBuffer buf2 = new CircularBuffer(64);
			    
			    CChannel localChannel = new CChannel(buf1, buf2);
			    CChannel remoteChannel = new CChannel(buf2, buf1);
			    
			    req.channel = remoteChannel;
			    synchronized(req) {
			    	req.notify();
			    }
				return localChannel;
			}
		}
		return state.channel;
		
	}
	
    private class ConnectRequest {
        CChannel channel;
    }

    private class PortState {
        boolean isAccepting = false;
        java.util.Queue<ConnectRequest> connectQueue = new java.util.LinkedList<>();
        CChannel channel;
    }

}
