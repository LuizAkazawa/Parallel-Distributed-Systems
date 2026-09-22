package edu.polytech.channels.empty;

import java.util.concurrent.ConcurrentHashMap;

public class BrokerManager {
	
	private static BrokerManager bm;
	private final ConcurrentHashMap<String, CBroker> tMap;

	BrokerManager() {
		tMap = new ConcurrentHashMap<>();
		bm = this;
	}
  
	public static BrokerManager getInstance() {
		return bm;
	}

	public void add(CBroker broker) {
		tMap.put(broker.getName(), broker);
	}
  
	public void remove(CBroker broker) {
		tMap.remove(broker.getName());
	}
  
	public CBroker get(String name) {
		return tMap.get(name);
	}
  
}
