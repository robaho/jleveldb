package com.robaho.jleveldb;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

class MemorySegment implements Segment {
    private ConcurrentSkipListMap<byte[],byte[]> list;
    private LogFile log;
    private long id;
    private int bytes;
    private String path;
    private Options options;

    MemorySegment(String path,long id,Options options) {
        list = new ConcurrentSkipListMap<>(KeyComparison.newKeyCompare(options));
        this.path = path;
        this.id = id;
        this.options = options;
    }

    static MemorySegment newMemoryOnlySegment() {
        return new MemorySegment("",0,new Options());
    }
    public long upperID() {
        return id;
    }
    public long lowerID() {
        return id;
    }

    int getBytes() {
        return bytes;
    }

    private void maybeCreateLogFile() throws IOException {
        if(log!=null || path.equals(""))
            return;
        log = new LogFile(path,id,options);
    }

    @Override
    public byte[] put(byte[] key, byte[] value) throws IOException {
        if(key==null || value==null)
            throw new IllegalArgumentException("null keys & values are not supported");
        maybeCreateLogFile();
        var prev = list.put(key, value);
        bytes += key.length + value.length - (prev!=null ? key.length + prev.length : 0);
        if(log!=null) {
            log.write(key,value);
        }
        return prev;
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        if(key==null)
            throw new IllegalArgumentException("null keys & values are not supported");
        byte[] value = list.get(key);
        if(value!=null && value.length==0)
            return null;
        return value;
    }

    @Override
    public byte[] remove(byte[] key) throws IOException {
        if(key==null)
            throw new IllegalArgumentException("null keys & values are not supported");
        return put(key,KeyValue.EMPTY);
    }

    @Override
    public void close() throws IOException {
    }
    public Collection<String> files() {
        if(log!=null) {
            return Collections.singleton(log.filepath.getFileName().toString());
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    public LookupIterator lookup(byte[] lower, byte[] upper) throws IOException {
        return getLookupIterator(lower, upper, list);
    }

    static LookupIterator getLookupIterator(byte[] lower, byte[] upper, ConcurrentSkipListMap<byte[], byte[]> list) {
        if(lower==null && upper==null)
            return new MemorySegmentIterator(list);

        if(lower==null){
            return new MemorySegmentIterator(list.headMap(upper,true));
        } else if(upper==null) {
            return new MemorySegmentIterator(list.tailMap(lower,true));
        } else {
            return new MemorySegmentIterator(list.subMap(lower,true,upper,true));
        }
    }

    private static Removable createRemovable(MemorySegment ms) {
        final LogFile log = ms.log;
        return new Removable() {
            @Override
            public void remove() throws IOException {
                if(log!=null)
                    log.remove();
            }
            public String toString() {
                return "LogFile:"+ log.filepath;
            }
        };

    }

    @Override
    public void removeOnFinalize() {
        Deleter.removeOnFinalize(this, createRemovable(this));
    }

    public void removeSegment() throws IOException {
        if(log!=null) {
            log.remove();
        }
    }
}

