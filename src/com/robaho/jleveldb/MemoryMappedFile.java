package com.robaho.jleveldb;

import sun.misc.Unsafe;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

final class UnsafeUtils {
    private static final Unsafe unsafe;

    public static Unsafe getUnsafe() {
        return unsafe;
    }

    static {
        Field f;
        try {
            f = Unsafe.class.getDeclaredField("theUnsafe");
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException("unable to get Unsafe",e);
        }

        f.setAccessible(true);

        try {
            unsafe = (Unsafe)f.get((Object)null);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("unable to get Unsafe",e);
        }
    }
}

public class MemoryMappedFile {
    private final int MAX_MAP_SIZE = 1024*1024*1024; // 1GB
    private final long length;
    private final ByteBuffer[] buffers;
    private boolean closed;
    private final FileChannel ch;

    public MemoryMappedFile(RandomAccessFile file) throws IOException {
        this.length = file.length();
        ch = file.getChannel();
        buffers = new ByteBuffer[(int)((length / MAX_MAP_SIZE)+1)];
        long temp = length;
        for(int i=0;i< buffers.length;i++){
            buffers[i] = ch.map(FileChannel.MapMode.READ_ONLY,i* MAX_MAP_SIZE,Math.min(temp, MAX_MAP_SIZE)).order(ByteOrder.LITTLE_ENDIAN);
            temp -= MAX_MAP_SIZE;
        }
    }
    long length(){
        return length;
    }

    /** read dst.capacity() of mapped file at position into the provided dst buffer at offset 0. dst is always cleared and flipped */
    public void readAt(byte[] dst, long position) throws IOException {
        readAt(dst,position,dst.length);
    }
    /** read n bytes of mapped file at position into the provided dst buffer at offset 0. dst is always cleared and flipped */
    public int readAt(byte[] dst, long position, int n) throws IOException {
        if(closed)
            throw new IOException("memory mapped file is closed");

        int count=0;
        while(n>0) {
            ByteBuffer b = buffers[(int) (position / MAX_MAP_SIZE)];
            int offset = (int) (position % MAX_MAP_SIZE);
            int len = Math.min(n,b.capacity()-offset);
            b.get(offset,dst,count,len);
            n-=len;
            count+=len;
        }
        return count;
    }

    public void close() throws IOException {
        for(ByteBuffer buffer : buffers) {
            UnsafeUtils.getUnsafe().invokeCleaner(buffer);
        }
        ch.close();
        closed = true;
    }
}
