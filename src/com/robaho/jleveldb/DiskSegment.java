package com.robaho.jleveldb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static com.robaho.jleveldb.Constants.*;

class DiskSegment implements Segment {
    final MemoryMappedFile keyFile;
    long keyBlocks;
    final MemoryMappedFile dataFile;
    final long lowerID;
    final long upperID;

    final private String keyfilename;
    final private String datafilename;

    // nil for segments loaded during initial open
    // otherwise holds the key for every keyIndexInterval block
    final List<byte[]> keyIndex;
    final long size;

    public DiskSegment(String keyFilename, String dataFilename, List<byte[]> keyIndex) throws IOException {
        this.keyfilename = keyFilename;
        this.datafilename = dataFilename;

        keyFile = new MemoryMappedFile(new RandomAccessFile(new File(keyFilename),"r"));
        dataFile = new MemoryMappedFile(new RandomAccessFile(dataFilename,"r"));
        this.lowerID = Utils.getSegmentIDs(keyFilename)[0];
        this.upperID = Utils.getSegmentIDs(keyFilename)[1];
        this.keyBlocks = (keyFile.length()-1)/keyBlockSize + 1;

        size = Files.size(Path.of(keyFilename))+Files.size(Path.of(dataFilename));

        if(keyIndex == null) {
            // TODO maybe load this in the background
            keyIndex = loadKeyIndex(keyFile, keyBlocks);
        }
        this.keyIndex = keyIndex;
    }

    public String toString() {
        return "DiskSegment:"+keyfilename+","+datafilename;
    }

    public long size() {
        return size;
    }

    static List<byte[]> loadKeyIndex(MemoryMappedFile keyFile,long keyBlocks) throws IOException {
        if(keyFile.length()==0) {
            return Collections.emptyList();
        }

        ByteBuffer bb = ByteBuffer.allocateDirect(keyBlockSize).order(ByteOrder.BIG_ENDIAN);
        List<byte[]> keyIndex = new ArrayList<byte[]>();
        long block;

        for(block = 0; block < keyBlocks; block += keyIndexInterval) {
            keyFile.readAt(bb,block*keyBlockSize,maxKeySize+2); // read start key in block
            int keylen = bb.getShort() & 0xFFFF;
            if(keylen == endOfBlock) {
                break;
            }
            byte[] keycopy = new byte[keylen];
            bb.get(keycopy);
            keyIndex.add(keycopy);
        }
        return keyIndex;
    }

    static List<Segment> loadDiskSegments(String path, Options options) throws IOException {
        List<Segment> segments = new ArrayList();
        File dir = new File(path);
        if(!dir.isDirectory()) {
            throw new IllegalStateException(""+path+" is not a directory");
        }

        // first remove any 'tmp' files and related non-temp files as this signifies
        // a failure during write
        for (var file : dir.listFiles()) {
            if(!file.getName().endsWith(".tmp")) {
                continue;
            }
            String base = Utils.trimSuffix(file.getName(),".tmp");
            String segs;
            if(base.startsWith("keys.")) {
                segs = Utils.trimPrefix(base, "keys.");
            } else {
                segs = Utils.trimPrefix(base, "data.");
            }
            Utils.removeFileIfExists(path,"keys."+segs);
            Utils.removeFileIfExists(path,"data."+segs);
            Utils.removeFileIfExists(path,"keys."+segs+".tmp");
            Utils.removeFileIfExists(path,"data."+segs+".tmp");
        }

        for (var file : dir.listFiles()) {
            if(file.getName().startsWith("log.")) {
                var ls = new LogSegment(file.getPath(),options);
                segments.add(ls);
                continue;
            }
            if(!file.getName().startsWith("keys.")) {
                continue;
            }
            String segs = Utils.trimPrefix(file.getName(),"keys.");
            String keyFilename = path+"/keys."+segs;
            String dataFilename = path+"/data."+segs;
            segments.add(new DiskSegment(keyFilename,dataFilename,null));
        }
        Collections.sort(segments, (o1, o2) -> {
            int result = Long.compare(o1.upperID(),o2.upperID());
            if(result==0) {
                // the only way this is possible is if we have a log file that has already been merged, but
                // wasn't deleted, so sort the log file first
                result =  Long.compare(o1.lowerID(),o2.lowerID()) * -1;
            }
            return result;
        });
        int pruneCount=0;
next:
        for (int i=0;i<segments.size();) {
            Segment seg = segments.get(i);
            for(int j=i+1;j<segments.size();j++) {
                Segment seg0 = segments.get(j);
                if(seg.lowerID() >= seg0.lowerID() && seg.upperID() <= seg0.upperID()) {
                    segments.remove(i);
                    seg.removeSegment();
                    pruneCount++;
                    continue next;
                }
            }
            i++;
        }
        if(pruneCount>0)
            System.out.println("pruned "+pruneCount+" segments at open");
        return segments;
    }

    @Override
    public byte[] put(byte[] key, byte[] value) throws IOException {
        throw new IllegalStateException("disk segments are immutable");
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        OffsetLen ol = binarySearch(key);
        if(ol==null)
            return null;

        byte[] buffer = new byte[ol.len];
        dataFile.readAt(ByteBuffer.wrap(buffer).order(ByteOrder.BIG_ENDIAN),ol.offset);
        return buffer;
    }

    @Override
    public byte[] remove(byte[] key) throws IOException {
        throw new IllegalStateException("disk segments are immutable");
    }

    @Override
    public void close() throws IOException {
        keyFile.close();
        dataFile.close();
    }

    public void removeSegment() throws IOException {
        close();
        Files.delete(Path.of(keyfilename));
        Files.delete(Path.of(datafilename));
    }
    // EMPTY is an inner class so that a reference to the parent segment is retained for debugging
    private final LookupIterator EMPTY = new LookupIterator() {
        public KeyValue next() throws IOException {
            return null;
        }
        public byte[] peekKey() throws IOException {
            return null;
        }
        public String toString() {
            return "EmptyIterator for "+DiskSegment.this;
        }
    };

    @Override
    public LookupIterator lookup(byte[] lower, byte[] upper) throws IOException {
        if(size()==0)
            return EMPTY;

        ByteBuffer buffer = ByteBuffer.allocateDirect(keyBlockSize).order(ByteOrder.BIG_ENDIAN);
        long block = 0;
        if(lower != null) {
            LowHigh lh = indexSearch(lower);
            if(lh==null)
                return EMPTY;
            long startBlock = binarySearch0(lh.low, lh.high, lower, buffer);
            if(startBlock<0)
                return null;
            block = startBlock;
        }
        keyFile.readAt(buffer,block* keyBlockSize);
        return new DiskSegmentIterator(this,lower,upper,buffer,block);
    }

    private static Removable createRemovable(DiskSegment ds) {
        final var keyfile = ds.keyFile;
        final var datafile = ds.dataFile;
        final var keyname = ds.keyfilename;
        final var dataname = ds.datafilename;
        return new Removable() {
            @Override
            public void remove() throws IOException {
                keyfile.close();
                datafile.close();
                Files.deleteIfExists(Path.of(keyname));
                Files.deleteIfExists(Path.of(dataname));
            }
            public String toString() {
                return "DiskSegment:"+keyname+","+dataname;
            }
        };
    }

    @Override
    public void removeOnFinalize() {
        Deleter.removeOnFinalize(this,createRemovable(this));
    }

    @Override
    public Collection<String> files() {
        return List.of(Utils.getFileName(keyfilename),Utils.getFileName(datafilename));
    }

    private static class OffsetLen {
        final long offset;
        final int len;
        OffsetLen(long offset,int len){
            this.offset = offset;
            this.len = len;
        }
    }

    private static class LowHigh {
        final long low, high;

        public LowHigh(long low, long high) {
            this.low=low;
            this.high=high;
        }
    }

    private static class ReadStats {
        int reads;
        int scanRows;
    }

    private OffsetLen binarySearch(byte[] key) throws IOException {
        ByteBuffer buffer = bufferCache.get();

        LowHigh lh = indexSearch(key);
        if(lh==null)
            return null;

        long block = binarySearch0(lh.low, lh.high, key, buffer);

        return scanBlock(block, key, buffer);
    }

    LowHigh indexSearch(byte[] key) {
        if(keyIndex==null) {
            return new LowHigh(0, keyBlocks - 1);
        }

        long lowblock,highblock;

        // we have memory index, so narrow block range down
        //            System.out.println("looking for "+new String(key));
        //            for(byte[] b : keyIndex) {
        //                System.out.println(new String(b));
        //            }

        int index = Collections.binarySearch(keyIndex,key,new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return Arrays.compare(o1,o2);
            }
        });
        if(index>=0) {
            highblock = lowblock = index* keyIndexInterval;
        } else {
            index = (index*-1) - 1;
            index--;
            index = Math.max(0,index);
            lowblock = index * keyIndexInterval;
            highblock = lowblock + keyIndexInterval;
        }

        if(highblock >= keyBlocks) {
            highblock = keyBlocks - 1;
        }
        return new LowHigh(lowblock,highblock);
    }

    private static int compareKeys(byte[] b,ByteBuffer bb){
        short len = bb.getShort();
        assert bb.remaining() >= len;
        for(int i=0;i<len;i++){
            if(i==b.length)
                return -1;
            int result = Byte.compare(b[i],bb.get());
            if(result!=0) {
                bb.position(bb.position()+len-i-1);
                return result;
            }
        }
        if(len<b.length)
            return 1;
        return 0;
    }

    long binarySearch0(long lowBlock, long highBlock, byte[] key, ByteBuffer buffer) throws IOException {
        if(highBlock-lowBlock <= 1) {
            // the key is either in low block or high block, or does not exist, so check high block
            keyFile.readAt(buffer, highBlock* keyBlockSize, maxKeySize+2);
            if(compareKeys(key,buffer)<0) {
                return lowBlock;
            } else {
                return highBlock;
            }
        }

        long block = (lowBlock+highBlock)/2;

        keyFile.readAt(buffer, block*Constants.keyBlockSize, maxKeySize+2);

        if(compareKeys(key,buffer)<0) {
            return binarySearch0(lowBlock, block-1, key, buffer);
        } else {
            return binarySearch0(block, highBlock, key, buffer);
        }
    }

    static final ThreadLocal<ByteBuffer> bufferCache = new ThreadLocal<>(){
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(keyBlockSize);
        }
    };
    OffsetLen scanBlock(long block, byte[] key, ByteBuffer buffer) throws IOException {
        keyFile.readAt(buffer, block*Constants.keyBlockSize,keyBlockSize);

        KeyBuffer currKey = new KeyBuffer();
        KeyBuffer prevKey = new KeyBuffer();

        for(;;) {
            int keylen = buffer.getShort() & 0xFFFF;
            if(keylen == endOfBlock) {
                return null;
            }
            CompressedKey.decodeKey(currKey,prevKey,keylen,buffer);

            int result = currKey.compare(key);
            if(result==0) {
                long offset = buffer.getLong();
                int len = buffer.getInt();
                return new OffsetLen(offset,len);
            }
            if(result>0) {
                return null;
            }
            buffer.position(buffer.position()+12);
            currKey.copyTo(prevKey);
        }
    }

    public long lowerID() {
        return lowerID;
    }
    public long upperID() {
        return upperID;
    }

}
