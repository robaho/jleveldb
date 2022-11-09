package com.robaho.jleveldb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.robaho.jleveldb.Constants.keyBlockSize;

class DiskSegmentIterator implements LookupIterator{
    private boolean isValid = false;
    private boolean finished = false;

    byte[] key;
    byte[] data;

    final DiskSegment segment;
    final ByteBuffer buffer;

    final byte[] lower,upper;

    final KeyBuffer currKey=new KeyBuffer();
    final KeyBuffer prevKey=new KeyBuffer();

    long block;

    DiskSegmentIterator(DiskSegment segment,byte[] lower,byte[] upper,ByteBuffer buffer,long block){
        this.segment = segment;
        this.lower = lower;
        this.upper = upper;
        this.buffer = buffer;
        this.block = block;
    }

    @Override
    public byte[] peekKey() throws IOException {
        if(isValid){
            return key;
        }
        if(nextKeyValue()) {
            return null;
        }
        return key;
    }

    @Override
    public KeyValue next() throws IOException {
        if(finished) {
            return null;
        }
        if(isValid) {
            isValid = false;
            return new KeyValue(key,data);
        }
        try {
            if (nextKeyValue())
                return null;

            return new KeyValue(key, data);
        } finally {
            isValid = false;
        }
    }

    /** returns true if no more values */
    private boolean nextKeyValue() throws IOException {
        if(finished) {
            return true;
        }
        prevKey.fromBytes(key);

        while(true) {
            int keylen = buffer.getShort() & 0xFFFF;
            if(keylen == Constants.endOfBlock) {
                block++;
                if (block == segment.keyBlocks) {
                    finished = true;
                    key = null;
                    data = null;
                    isValid = true;
                    return true;
                }
                segment.keyFile.readAt(buffer,block* keyBlockSize);
                if(buffer.remaining()!=keyBlockSize)
                    throw new IOException("unable to read keyfile");
                prevKey.clear();
                continue;
            }

            CompressedKey.decodeKey(currKey,prevKey,keylen,buffer);
            currKey.copyTo(prevKey);

            long dataoffset = buffer.getLong();
            int datalen = buffer.getInt();

            if(lower != null) {
                if (currKey.compare(lower)<0) {
                    continue;
                }
            }
            if(upper != null) {
                if(currKey.compare(upper)>0) {
                    finished = true;
                    isValid = true;
                    key = null;
                    data = null;
                    return true;
                }
            }
            found:

            key = currKey.toBytes();
            data = new byte[datalen];
            ByteBuffer bb = ByteBuffer.wrap(data);
            segment.dataFile.readAt(bb,dataoffset);
            if(bb.remaining()!=datalen)
                throw new IOException("unable to read data file, expecting "+datalen+", read "+(bb.capacity()-bb.remaining()));
            isValid = true;
            return false;
        }
    }


}


