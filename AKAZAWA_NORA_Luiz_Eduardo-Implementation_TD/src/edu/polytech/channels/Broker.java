package edu.polytech.channels;

public interface Broker {
    Channel accept(int port);
    Channel connect(String name, int port);
    String getName();
}
