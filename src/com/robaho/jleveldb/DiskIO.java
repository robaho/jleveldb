package com.robaho.jleveldb;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

class DiskIO {
    // called to write a memory segment to disk. If the segment is empty, null is returned.
    static DiskSegment writeSegmentToDisk(String dbPath,MemorySegment seg) throws IOException {
        var itr = seg.lookup(null,null);

        if (itr.peekKey()==null) {
            seg.removeSegment();
            return null;
        }

        var lowerId = seg.lowerID();
        var upperId = seg.upperID();

        var keyFilename = String.format("%s/keys.%d.%d",dbPath,lowerId,upperId);
        var dataFilename = String.format("%s/data.%d.%d",dbPath,lowerId,upperId);

        DiskSegment ds = writeAndLoadSegment(keyFilename, dataFilename, itr, false);
        seg.removeSegment();
        return ds;
    }

    static DiskSegment writeAndLoadSegment(String keyFilename, String dataFilename,LookupIterator itr,boolean removeDeleted) throws IOException {
        var keyFileTmp = new File(keyFilename + ".tmp");
        var dataFileTmp = new File(dataFilename + ".tmp");

        List<byte[]> keyIndex = null;
        try {
            keyIndex = writeSegmentFiles(keyFileTmp, dataFileTmp, itr, removeDeleted);
        } catch (IOException e) {
            keyFileTmp.delete();
            dataFileTmp.delete();
            throw e;
        }

        keyFileTmp.renameTo(new File(keyFilename));
        dataFileTmp.renameTo(new File(dataFilename));

        return new DiskSegment(keyFilename, dataFilename, keyIndex);
    }

    static List<byte[]> writeSegmentFiles(File keyFile, File dataFile,LookupIterator itr,boolean removeDeleted) throws IOException {
        var keyW = new LittleEndianDataOutputStream(new BufferedOS(new FileOutputStream(keyFile)));
        var dataW = new LittleEndianDataOutputStream(new BufferedOS(new FileOutputStream(dataFile)));

        long dataOffset=0;
        int keyBlockLen=0;
        int keyCount=0;
        int block=0;

        byte[] zeros = new byte[Constants.keyBlockSize];
        byte[] prevKey = null;

        List<byte[]> keyIndex = new ArrayList<>();

        while (true) {
            KeyValue kv = itr.next();
            if(kv==null)
                break;

            byte[] value = kv.value;
            byte[] key = kv.key;

            if(removeDeleted && kv.value.length==0) {
                continue;
            }

            keyCount++;

            dataW.write(value);
            if (keyBlockLen + 2 + key.length + 8 + 4 >= Constants.keyBlockSize - 2) {
                // need to leave room for 'end of block marker'
                // key won't fit in block so move to next
                keyW.writeShort(Constants.endOfBlock);
                keyBlockLen += 2;
                keyW.write(zeros, 0, Constants.keyBlockSize - keyBlockLen);
                keyBlockLen = 0;
                prevKey = null;
            }

            if (keyBlockLen == 0) {
                if (block % Constants.keyIndexInterval == 0) {
                    keyIndex.add(key.clone());
                }
                block++;
            }

            int dataLen = value.length;

            var dk = encodeKey(key, prevKey);
            prevKey = key.clone();
            keyW.writeShort(dk.keylen);
            keyW.write(dk.compressedKey);
            keyW.writeLong(dataOffset);
            keyW.writeInt(dataLen);

            keyBlockLen += 2 + dk.compressedKey.length + 8 + 4;
            if (value != null) {
                dataOffset += dataLen;
            }
        }

        // pad key file to block size
        if (keyBlockLen > 0 && keyBlockLen < Constants.keyBlockSize) {
            // key won't fit in block so move to next
            keyW.writeShort(Constants.endOfBlock);
            keyBlockLen += 2;
            keyW.write(zeros, 0, Constants.keyBlockSize - keyBlockLen);
        }

        keyW.flush();
        dataW.flush();

        return keyIndex;
    }

    private static DiskKey encodeKey(byte[] key,byte[] prevKey) {
       int  prefixLen = calculatePrefixLen(prevKey, key);
        if (prefixLen > 0) {
            var key_ = new byte[key.length-prefixLen];
            System.arraycopy(key,prefixLen,key_,0,key_.length);
            return new DiskKey(Constants.compressedBit | (prefixLen<<8) | key_.length,key_);
        }
        return new DiskKey(key.length,key);
    }

    private static class DiskKey {
        final int keylen;
        final byte[] compressedKey;

        public DiskKey(int keylen, byte[] key) {
            this.keylen = keylen;
            this.compressedKey = key;
        }
    };

    private static int calculatePrefixLen(byte[] prevKey,byte[] key) {
        if(prevKey == null) {
            return 0;
        }
        int length = 0;

        for( ; length < prevKey.length && length < key.length; length++) {
            if(prevKey[length] != key[length]) {
                break;
            }
        }
        if(length > Constants.maxPrefixLen || key.length-length > Constants.maxCompressedLen) {
            length = 0;
        }
        return length;
    }
}
