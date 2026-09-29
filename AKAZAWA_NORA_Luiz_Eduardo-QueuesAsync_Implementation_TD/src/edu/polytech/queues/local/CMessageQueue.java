package edu.polytech.queues.local;

import edu.polytech.queues.MessageQueue;
import edu.polytech.queues.QueueBroker;
import edu.polytech.queues.Task;
import edu.polytech.utils.Executor;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class CMessageQueue implements MessageQueue {
    private final CQueueBroker broker;
    private CMessageQueue peer;
    
    private volatile Listener listener;
    private volatile Task listenerTask;
    
    private volatile boolean closed = false;
    private volatile boolean remoteClosed = false;
    
    private final Queue<byte[]> pendingMessages = new ConcurrentLinkedQueue<>();
    private volatile boolean pendingClose = false;

    public CMessageQueue(CQueueBroker broker) {
        this.broker = broker;
        Executor.self().register(broker.getTask(), this);
    }
    
    public void setPeer(CMessageQueue peer) {
        this.peer = peer;
    }

    @Override
    public QueueBroker broker() {
        return broker;
    }

    @Override
    public boolean send(byte[] bytes, int offset, int length, SendListener l) {
        if (closed || remoteClosed) {
            Task.task().post(() -> l.sent(bytes, offset, length));
            return false;
        }
        
        byte[] payload = Arrays.copyOfRange(bytes, offset, offset + length);
        
        peer.receivePayload(payload);
        
        Task.task().post(() -> l.sent(bytes, offset, length));
        return true;
    }
    
    protected void receivePayload(byte[] payload) {
        if (closed) return;
        
        if (listenerTask != null) {
            listenerTask.post(() -> {
                if (!closed) listener.received(payload);
            });
        } else {
            pendingMessages.add(payload);
        }
    }

    @Override
    public void setListener(Listener l) {
        this.listener = l;
        this.listenerTask = Task.task();
        
        while (!pendingMessages.isEmpty()) {
            byte[] payload = pendingMessages.poll();
            if (payload != null) {
                listenerTask.post(() -> {
                    if (!closed) listener.received(payload);
                });
            }
        }
        
        if (pendingClose) {
            listenerTask.post(() -> handleRemoteClose());
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        
        if (peer != null) {
            peer.triggerRemoteClose();
        }
    }

    @Override
    public boolean closed() {
        return closed;
    }
    

    protected void triggerRemoteClose() {
        if (listenerTask != null) {
            listenerTask.post(() -> handleRemoteClose());
        } else {
            pendingClose = true;
        }
    }
    
    protected void handleRemoteClose() {
        remoteClosed = true;
        if (listener != null) {
            listener.closed();
        }
        
        if (!closed) {
            close();
        }
    }
}
