package edu.polytech.utils;

public class CircularBuffer {
    volatile int m_tail, m_head;
    volatile byte m_bytes[];

    public CircularBuffer(int capacity) {
        m_bytes = new byte[capacity];
        m_tail = m_head = 0;
    }

    public boolean full() {
        int next = (m_head + 1) % m_bytes.length;
        return (next == m_tail);
    }

    public boolean empty() {
        return (m_tail == m_head);
    }

    public void push(byte b) {
        int next = (m_head + 1) % m_bytes.length;
        if (next == m_tail)
            throw new IllegalStateException();
        m_bytes[m_head] = b;
        m_head = next;
    }

    public byte pull() {
        if (m_tail == m_head)
            throw new IllegalStateException();
        int next = (m_tail + 1) % m_bytes.length;
        byte bits = m_bytes[m_tail];
        m_tail = next;
        return bits;
    }
}
