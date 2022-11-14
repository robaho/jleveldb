package com.robaho.jleveldb;

import java.io.*;
import java.util.Objects;

/** DataInputStream in Little Endian format. Not safe for concurrent use. */
public class LittleEndianDataInputStream extends ByteArrayInputStream {
    public LittleEndianDataInputStream(byte[] b) {
        super(b);
    }
    public LittleEndianDataInputStream(byte[] b,int offset,int len) {
        super(b,offset,len);
    }
    public final void readFully(byte b[], int off, int len) throws IOException {
        if (len < 0)
            throw new IndexOutOfBoundsException();
        int n = 0;
        while (n < len) {
            int count = read(b, off + n, len - n);
            if (count < 0)
                throw new EOFException();
            n += count;
        }
    }
    public final short readShort() throws IOException {
        int ch2 = (buf[pos++] & 0xff);
        int ch1 = (buf[pos++] & 0xff);
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (short)((ch1 << 8) + (ch2 << 0));
    }
    public final int readInt() throws IOException {
        int ch4 = read();
        int ch3 = read();
        int ch2 = read();
        int ch1 = read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }
    private byte readBuffer[] = new byte[8];

    public final long readLong() throws IOException {
        readFully(readBuffer, 0, 8);
        return (((long)readBuffer[7] << 56) +
                ((long)(readBuffer[6] & 255) << 48) +
                ((long)(readBuffer[5] & 255) << 40) +
                ((long)(readBuffer[4] & 255) << 32) +
                ((long)(readBuffer[3] & 255) << 24) +
                ((readBuffer[2] & 255) << 16) +
                ((readBuffer[1] & 255) <<  8) +
                ((readBuffer[0] & 255) <<  0));
    }

    public final long skip(long n) {
        pos+=n;
        return n;
    }

    public final int read(byte b[], int off, int len) {
        System.arraycopy(buf, pos, b, off, len);
        pos += len;
        return len;
    }
}
