package com.robaho.jleveldb;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/** used to efficiently handle prefix key compression */
final class KeyBuffer {
    final byte[] buffer = new byte[1024];
    int len=0;

    KeyBuffer(){}

    int compare(byte[] bytes) {
        return Arrays.compare(buffer,0,len,bytes,0,bytes.length);
    }
    public void from(InputStream src, int len, int offset) throws IOException {
        src.read(buffer,offset,len);
        this.len=offset+len;
    }

    public void clear() {
        len=0;
    }

    public byte[] toBytes() {
        byte[] bytes = new byte[len];
        System.arraycopy(buffer,0,bytes,0,len);
        return bytes;
    }
}
