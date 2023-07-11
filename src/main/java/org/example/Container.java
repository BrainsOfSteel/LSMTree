package org.example;

public class Container {
    private long timestamp;
    private int value;

    public Container(long timestamp, int value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public Container() {
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
