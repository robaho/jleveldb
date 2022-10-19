package com.robaho.jleveldb;

import java.nio.ByteBuffer;
import java.util.Arrays;

final class KeyBuffer {
    final byte[] buffer = new byte[1024];
    int len;

    public KeyBuffer(byte[] key) {
        if(key==null)
            return;
        System.arraycopy(key,0,buffer,0,key.length);
        this.len = key.length;
    }
    public KeyBuffer(){}

    void copyTo(KeyBuffer dst) {
        System.arraycopy(buffer,0,dst.buffer,0,len);
        dst.len=len;
    }
    int compare(byte[] bytes) {
        return Arrays.compare(buffer,0,len,bytes,0,bytes.length);
    }
    int compare(KeyBuffer to) {
        return Arrays.compare(buffer,0,len,to.buffer,0, to.len);
    }
    public void from(ByteBuffer src, int len) {
        src.get(buffer,0,len);
        this.len=len;
    }
    public void insertPrefix(KeyBuffer prefixKey, int prefixLen) {
        System.arraycopy(buffer,0,buffer,prefixLen,len);
        System.arraycopy(prefixKey.buffer,0,buffer,0,prefixLen);
        this.len+=prefixLen;
    }

    public void clear() {
        len=0;
    }

    public byte[] toBytes() {
        byte[] bytes = new byte[len];
        System.arraycopy(buffer,0,bytes,0,len);
        return bytes;
    }

    public void fromBytes(byte[] bytes) {
        if(bytes==null) {
            len=0;
            return;
        }
        System.arraycopy(buffer,0,bytes,0,bytes.length);
        len=bytes.length;
    }
}
