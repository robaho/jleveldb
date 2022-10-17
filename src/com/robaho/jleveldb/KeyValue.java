package com.robaho.jleveldb;

public class KeyValue {
    static final byte[] EMPTY = new byte[0];
    public final byte[] key;
    public final byte[] value;
    public KeyValue(byte[] key, byte[] value) {
        this.key=key;
        this.value=value;
    }
    public KeyValue(byte[] key) {
        this.key=key;
        this.value=EMPTY;
    }
}
