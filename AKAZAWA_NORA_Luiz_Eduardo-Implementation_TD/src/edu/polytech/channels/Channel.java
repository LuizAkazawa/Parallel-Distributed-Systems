package edu.polytech.channels;

public interface Channel {
    int read(byte[] bytes, int offset, int length);
    
    int write(byte[] bytes, int offset, int length);
    
    void disconnect();
    
    boolean disconnected();
}
