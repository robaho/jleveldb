package com.robaho.jleveldb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

interface Segment {
    long upperID();
    long lowerID();
    byte[] put(byte[] key,byte[] value) throws IOException;
    byte[] get(byte[] key) throws IOException;
    byte[] remove(byte[] key) throws IOException;
    void close() throws IOException;
    LookupIterator lookup(byte[] lower,byte[] upper) throws IOException;
    static List<Segment> copyAndAppend(List<Segment> list, Segment segment) {
        List<Segment> copy = new ArrayList<>(list.size()+1);
        copy.addAll(list);
        copy.add(segment);
        return copy;
    }
    void removeSegment() throws IOException;
    void removeOnFinalize();
    Collection<String> files();
    long size();
}
