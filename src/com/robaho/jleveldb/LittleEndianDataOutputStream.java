package com.robaho.jleveldb;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;


/** similar to DataOutputStream but uses LittleEndian byte order */
public class LittleEndianDataOutputStream extends FilterOutputStream {
    private final byte[] writeBuffer = new byte[8];

    public LittleEndianDataOutputStream(OutputStream out) {
        super(out);
    }
    public final void writeShort(int v) throws IOException {
        writeBuffer[1] = (byte)(v >>> 8);
        writeBuffer[0] = (byte)(v >>> 0);
        out.write(writeBuffer, 0, 2);
    }
    public final void writeInt(int v) throws IOException {
        writeBuffer[3] = (byte)(v >>> 24);
        writeBuffer[2] = (byte)(v >>> 16);
        writeBuffer[1] = (byte)(v >>>  8);
        writeBuffer[0] = (byte)(v >>>  0);
        out.write(writeBuffer, 0, 4);
    }
    public final void writeLong(long v) throws IOException {
        writeBuffer[7] = (byte)(v >>> 56);
        writeBuffer[6] = (byte)(v >>> 48);
        writeBuffer[5] = (byte)(v >>> 40);
        writeBuffer[4] = (byte)(v >>> 32);
        writeBuffer[3] = (byte)(v >>> 24);
        writeBuffer[2] = (byte)(v >>> 16);
        writeBuffer[1] = (byte)(v >>>  8);
        writeBuffer[0] = (byte)(v >>>  0);
        out.write(writeBuffer, 0, 8);
    }
}
