package org.pilot.filesystem;

import java.io.Serializable;

class FileOperation implements Serializable {
    private final long timestamp;
    private final long offset;
    private final byte[] data;
    private final int length;
    private final String operationType; // "WRITE" or "TRUNCATE"

    public FileOperation(long offset, byte[] data, String operationType) {
        this.timestamp = System.currentTimeMillis();
        this.offset = offset;
        this.data = data;
        this.length = data != null ? data.length : 0;
        this.operationType = operationType;
    }

    // Getters
    public long getTimestamp() { return timestamp; }
    public long getOffset() { return offset; }
    public byte[] getData() { return data; }
    public int getLength() { return length; }
    public String getOperationType() { return operationType; }

    public String toString() {
        return "FileOperation{" +
                "timestamp=" + timestamp +
                ", offset=" + offset +
                ", length=" + length +
                ", operationType='" + operationType + '\'' +
                '}';
    }
}

