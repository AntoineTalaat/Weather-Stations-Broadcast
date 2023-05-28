package com.example.centralstationkafka.bitcaskAndParquet;

import java.util.Arrays;

public class ByteArrayWrapper {
    private final byte[] bytes;

    public ByteArrayWrapper(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof ByteArrayWrapper other) {
            return Arrays.equals(this.bytes, other.bytes);
        }
        return false;
    }
}
