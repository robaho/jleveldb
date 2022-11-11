package com.robaho.jleveldb;

import java.nio.ByteBuffer;
import java.util.Arrays;

final class KeyBuffer {
    final byte[] buffer = new byte[1024];
    int len=0;
    int offset=1024;

    public KeyBuffer(byte[] key) {
        if(key==null)
            return;
        offset = 1024-key.length;
        len = key.length;
        System.arraycopy(key,0,buffer,offset,len);
    }
    public KeyBuffer(){}

    void copyTo(KeyBuffer dst) {
        dst.len=len;
        dst.offset=offset;
        System.arraycopy(buffer,offset,dst.buffer,offset,len);
    }
    int compare(byte[] bytes) {
        return Arrays.compare(buffer,offset,offset+len,bytes,0,bytes.length);
    }
    int compare(KeyBuffer to) {
        return Arrays.compare(buffer,offset,offset+len,to.buffer,to.offset, to.offset+to.len);
    }
    public void from(ByteBuffer src, int len) {
        offset=1024-len;
        src.get(buffer,offset,len);
        this.len=len;
    }
    public void insertPrefix(KeyBuffer prefixKey, int prefixLen) {
        offset-=prefixLen;
        System.arraycopy(prefixKey.buffer,prefixKey.offset,buffer,offset,prefixLen);
        this.len+=prefixLen;
    }

    public void clear() {
        len=0;offset=1024;
    }

    public byte[] toBytes() {
        byte[] bytes = new byte[len];
        System.arraycopy(buffer,offset,bytes,0,len);
        return bytes;
    }

    public void fromBytes(byte[] bytes) {
        if(bytes==null) {
            len=0;offset=1024;
            return;
        }
        len=bytes.length;
        offset = 1024-len;
        System.arraycopy(bytes,0,buffer,offset,len);
    }
}
