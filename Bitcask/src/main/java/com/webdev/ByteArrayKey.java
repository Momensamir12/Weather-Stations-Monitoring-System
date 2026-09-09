package com.webdev;

import java.util.Arrays;
import java.util.Objects;

public final class ByteArrayKey {

    private final byte [] key;


    public ByteArrayKey(byte[] key) {
        this.key = key.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ByteArrayKey that = (ByteArrayKey) o;
        return Objects.deepEquals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(key);
    }
}
