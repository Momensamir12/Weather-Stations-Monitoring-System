package com.webdev;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

public final class ByteArrayKey {

    private final byte[] key;

    public ByteArrayKey(byte[] key) {
        this.key = key.clone();
    }

    public byte[] getKey() {
        return key.clone(); // defensive copy, keeps the class's immutability intact
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

    @Override
    public String toString() {
        return new String(key, StandardCharsets.UTF_8); // debug-only; assumes text keys
    }
}