package edu.polytech.queues.local;

import java.util.concurrent.ConcurrentHashMap;
import edu.polytech.queues.QueueBroker;

public class BrokerManager {
    private static BrokerManager instance;
    private final ConcurrentHashMap<String, QueueBroker> brokers;

    public BrokerManager() {
        this.brokers = new ConcurrentHashMap<>();
        instance = this;
    }

    public static BrokerManager getInstance() {
        return instance;
    }

    public void register(QueueBroker broker) {
        brokers.put(broker.getName(), broker);
    }

    public void unregister(QueueBroker broker) {
        brokers.remove(broker.getName());
    }

    public QueueBroker get(String name) {
        return brokers.get(name);
    }
}
