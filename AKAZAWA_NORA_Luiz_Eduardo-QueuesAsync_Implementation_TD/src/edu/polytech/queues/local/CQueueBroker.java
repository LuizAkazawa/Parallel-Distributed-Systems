package edu.polytech.queues.local;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import edu.polytech.queues.QueueBroker;
import edu.polytech.queues.Task;

public class CQueueBroker implements QueueBroker {
    private final String name;
    private final Task ownerTask;
    
    private final ConcurrentHashMap<Integer, BindListener> boundPorts; // server arrives first
    private final ConcurrentHashMap<Integer, Queue<ConnectRequest>> pendingConnections; // client arrives first

    private static class ConnectRequest {
        Task clientTask;
        ConnectListener listener;
        CQueueBroker clientBroker;
        ConnectRequest(Task t, ConnectListener l, CQueueBroker cb) {
            this.clientTask = t;
            this.listener = l;
            this.clientBroker = cb;
        }
    }

    public CQueueBroker(String name) {
        this.name = name;
        this.ownerTask = Task.task(); 
        this.boundPorts = new ConcurrentHashMap<>();
        this.pendingConnections = new ConcurrentHashMap<>();
        
        BrokerManager.getInstance().register(this);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Task getTask() {
        return ownerTask;
    }

    @Override
    public boolean bind(int port, BindListener listener) {
        boundPorts.put(port, listener);
        
        Queue<ConnectRequest> pending = pendingConnections.get(port);
        if (pending != null) {
            while (!pending.isEmpty()) {
                ConnectRequest req = pending.poll();
                if (req != null) {
                    establishConnection(port, listener, req);
                }
            }
        }
        return true;
    }

    @Override
    public boolean unbind(int port) {
        BindListener listener = boundPorts.remove(port);
        if (listener != null) {
            ownerTask.post(() -> listener.unbound());
            return true;
        }
        return false;
    }

    @Override
    public boolean connect(String name, int port, ConnectListener listener) {
        CQueueBroker remoteBroker = (CQueueBroker) BrokerManager.getInstance().get(name);
        if (remoteBroker == null) {
            return false;
        }
        
        remoteBroker.handleConnect(port, listener, Task.task(), this);
        return true;
    }
    
    private synchronized void handleConnect(int port, ConnectListener listener, Task clientTask, CQueueBroker clientBroker) {
        BindListener bindListener = boundPorts.get(port);
        if (bindListener != null) {
            ConnectRequest req = new ConnectRequest(clientTask, listener, clientBroker);
            establishConnection(port, bindListener, req);
        } else {
            pendingConnections.putIfAbsent(port, new ConcurrentLinkedQueue<>());
            pendingConnections.get(port).add(new ConnectRequest(clientTask, listener, clientBroker));
        }
    }
    
    private void establishConnection(int port, BindListener bindListener, ConnectRequest req) {
        CMessageQueue serverQueue = new CMessageQueue(this);
        CMessageQueue clientQueue = new CMessageQueue(req.clientBroker);
        
        serverQueue.setPeer(clientQueue);
        clientQueue.setPeer(serverQueue);
        
        this.ownerTask.post(() -> bindListener.accepted(serverQueue));
        
        req.clientTask.post(() -> req.listener.connected(clientQueue));
    }
}
