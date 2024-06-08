package com.robaho.jleveldb;

import java.io.ByteArrayInputStream;
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
    final byte[] buffer;

    final byte[] lower,upper;

    final KeyBuffer currKey=new KeyBuffer();

    long block;

    LittleEndianDataInputStream is;

    DiskSegmentIterator(DiskSegment segment,byte[] lower,byte[] upper,byte[] buffer,long block){
        this.segment = segment;
        this.lower = lower;
        this.upper = upper;
        this.buffer = buffer;
        this.block = block;
        is = new LittleEndianDataInputStream(buffer);
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

        while(true) {
            int keylen = is.readShort() & 0xFFFF;
            if(keylen == Constants.endOfBlock) {
                block++;
                if (block == segment.keyBlocks) {
                    finished = true;
                    key = null;
                    data = null;
                    isValid = true;
                    return true;
                }
                int len = segment.keyFile.readAt(buffer,block* keyBlockSize, keyBlockSize);
                is = new LittleEndianDataInputStream(buffer,0,len);
                if(is.available()!=keyBlockSize)
                    throw new IOException("unable to read keyfile");
                continue;
            }

            CompressedKey.decodeKey(currKey,keylen,is);

            long dataoffset = is.readLong();
            int datalen = is.readInt();

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
            int result = segment.dataFile.readAt(data,dataoffset,datalen);
            if(result!=datalen) {
                throw new IOException("unable to read data file, expecting " + datalen + ", read " + result);
            }
            isValid = true;
            return false;
        }
    }


}


