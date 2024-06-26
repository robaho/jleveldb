package com.robaho.jleveldb;

import java.io.ByteArrayInputStream;
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
        String keyRange="";
        if(keyIndex!=null && keyIndex.size()>0) {
            keyRange = new String(keyIndex.get(0)) + "<>"+(new String(keyIndex.get(keyIndex.size()-1)));
        }
        return "DiskSegment:"+keyfilename+","+datafilename+":"+keyRange;
    }

    public long size() {
        return size;
    }

    static List<byte[]> loadKeyIndex(MemoryMappedFile keyFile,long keyBlocks) throws IOException {
        if(keyFile.length()==0) {
            return Collections.emptyList();
        }

        byte[] buffer = new byte[keyBlockSize];
        List<byte[]> keyIndex = new ArrayList<byte[]>();
        long block;

        for(block = 0; block < keyBlocks; block += keyIndexInterval) {
            keyFile.readAt(buffer,block*keyBlockSize,maxKeySize+2); // read start key in block
            LittleEndianDataInputStream is = new LittleEndianDataInputStream(buffer);
            int keylen = is.readShort() & 0xFFFF;
            if(keylen == endOfBlock) {
                break;
            }
            byte[] keycopy = new byte[keylen];
            is.readFully(keycopy,0,keylen);
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
        dataFile.readAt(buffer,ol.offset);
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

        ScanContext ctx = new ScanContext();
        long block = 0;
        if(lower != null) {
            LowHigh lh = indexSearch(lower);
            if(lh==null)
                return EMPTY;
            long startBlock = binarySearch0(lh.low, lh.high, lower, ctx);
            if(startBlock<0)
                return null;
            block = startBlock;
        }
        keyFile.readAt(ctx.buffer,block* keyBlockSize);
        return new DiskSegmentIterator(this,lower,upper,ctx.buffer,block);
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
        ScanContext ctx = bufferCache.get();
        ctx.reset();

        LowHigh lh = indexSearch(key);
        if(lh==null)
            return null;

        long block = binarySearch0(lh.low, lh.high, key, ctx);

        return scanBlock(block, key, ctx);
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

    private static int compareKeys(byte[] b,byte[] buffer){
        short len = (short) (buffer[0]<<0+(buffer[1]<<8));
        int offset=2;
        for(int i=0;i<len;i++){
            if(i==b.length)
                return -1;
            int result = Byte.compare(b[i],buffer[offset+i]);
            if(result!=0) {
                return result;
            }
        }
        if(len<b.length)
            return 1;
        return 0;
    }

    long binarySearch0(long lowBlock, long highBlock, byte[] key, ScanContext ctx) throws IOException {
        if(highBlock-lowBlock <= 1) {
            // the key is either in low block or high block, or does not exist, so check high block
            keyFile.readAt(ctx.buffer, highBlock* keyBlockSize, maxKeySize+2);
            if(compareKeys(key,ctx.buffer)<0) {
                return lowBlock;
            } else {
                return highBlock;
            }
        }

        long block = (lowBlock+highBlock)/2;

        keyFile.readAt(ctx.buffer, block*Constants.keyBlockSize, maxKeySize+2);

        if(compareKeys(key,ctx.buffer)<0) {
            return binarySearch0(lowBlock, block-1, key, ctx);
        } else {
            return binarySearch0(block, highBlock, key, ctx);
        }
    }

    private static class ScanContext {
        final KeyBuffer key = new KeyBuffer();
        final byte[] buffer = new byte[keyBlockSize];
        final LittleEndianDataInputStream is = new LittleEndianDataInputStream(buffer);
        void reset() {
            key.clear();
            is.reset();
        }
    }

    static final ThreadLocal<ScanContext> bufferCache = new ThreadLocal<>(){
        @Override
        protected ScanContext initialValue() {
            return new ScanContext();
        }
    };
    OffsetLen scanBlock(long block, byte[] key,ScanContext ctx) throws IOException {
        ctx.reset();

        keyFile.readAt(ctx.buffer, block*Constants.keyBlockSize,keyBlockSize);

        LittleEndianDataInputStream is = ctx.is;

        KeyBuffer currKey = ctx.key;

        for(;;) {
            int keylen = is.readShort() & 0xFFFF;
            if(keylen == endOfBlock) {
                return null;
            }
            CompressedKey.decodeKey(currKey,keylen,is);

            int result = currKey.compare(key);
            if(result==0) {
                long offset = is.readLong();
                int len = is.readInt();
                return new OffsetLen(offset,len);
            }
            if(result>0) {
                return null;
            }
            is.skip(12);
        }
    }

    public long lowerID() {
        return lowerID;
    }
    public long upperID() {
        return upperID;
    }

}
