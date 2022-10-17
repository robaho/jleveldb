package com.robaho.jleveldb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentSkipListMap;

public class LogSegment implements Segment {
    private ConcurrentSkipListMap<byte[],byte[]> list;
    private long id;
    private String filepath;
    private Options options;

    public LogSegment(String filepath,Options options) throws IOException {
        list = LogFile.readLogFile(filepath,options);
        id = Utils.getSegmentID(filepath);
        this.filepath = filepath;
        this.options = options;
    }
    public long lowerID() {
        return id;
    }
    public long upperID() {
        return id;
    }
    public byte[] get(byte[] key) throws IOException {
        return list.get(key);
    }
    public byte[] put(byte[] key,byte[] value) throws IOException {
        throw new IllegalStateException("put() called on immutable segment");
    }
    public void write(WriteBatch batch) throws IOException {
        throw new IllegalStateException("write() called on immutable segment");
    }
    public byte[] remove(byte[] key) throws IOException {
        throw new IllegalStateException("remove() called on immutable segment");
    }
    public void close() throws IOException {
    }
    public void removeSegment() throws IOException {
        close();
        Files.delete(Path.of(filepath));
    }

    public LookupIterator lookup(byte[] lower, byte[] upper) throws IOException {
        return MemorySegment.getLookupIterator(lower, upper, list);
    }

    @Override
    public void removeOnFinalize() {
        Deleter.removeOnFinalize(this);
    }

    @Override
    public Collection<String> files() {
        return Collections.singleton(new File(filepath).getName());
    }
}
