package com.robaho.jleveldb;

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class MergerTest extends TestCase {
    static Deleter newNullDeleter() {
        return new Deleter(){
            @Override
            public void scheduleDeletion(List<String> filesToDelete) throws IOException {
            }
            @Override
            public void deleteScheduled() throws IOException {
            }
        };
    }

    public void testMerger() throws IOException {
        File dir = new File("testdb");
        dir.mkdir();
        IOUtils.purgeDirectory(dir);
        var m1 = MemorySegment.newMemoryOnlySegment();
        for (int i = 0; i < 100000; i++) {
            m1.put(("mykey" + i).getBytes(), ("myvalue" + i).getBytes());
        }
        var m2 = MemorySegment.newMemoryOnlySegment();
        for (int i = 100000; i < 200000; i++) {
            m2.put(("mykey" + i).getBytes(), ("myvalue" + i).getBytes());
        }
        var merged = Merger.mergeSegments1(newNullDeleter(), "testdb", List.of(m1, m2), true);
        var itr = merged.lookup(null, null);
        int count = 0;
        while (true) {
            KeyValue kv = itr.next();
            if (kv == null)
                break;
            count++;
        }
        assertEquals(200000, count);
    }
    public void testMergerRemove() throws IOException {
        File dir = new File("testdb");
        dir.mkdir();
        IOUtils.purgeDirectory(dir);
        var m1 = MemorySegment.newMemoryOnlySegment();
        for (int i = 0; i < 100000; i++) {
            m1.put(("mykey" + i).getBytes(), ("myvalue" + i).getBytes());
        }
        var m2 = MemorySegment.newMemoryOnlySegment();
        for (int i = 0; i < 100000; i++) {
            m2.remove(("mykey" + i).getBytes());
        }
        var merged = Merger.mergeSegments1(newNullDeleter(), "testdb", List.of(m1, m2), false);
        var itr = merged.lookup(null, null);
        int count=0;
        for(;;) {
            KeyValue kv = itr.next();
            if(kv==null)
                break;
            count++;
        }
        assertEquals(count,100000);
    }
    public void testMergerRemoveWithPurge() throws IOException {
        File dir = new File("testdb");
        dir.mkdir();
        IOUtils.purgeDirectory(dir);
        var m1 = MemorySegment.newMemoryOnlySegment();
        for (int i = 0; i < 100000; i++) {
            m1.put(("mykey" + i).getBytes(), ("myvalue" + i).getBytes());
        }
        var m2 = MemorySegment.newMemoryOnlySegment();
        for (int i = 0; i < 100000; i++) {
            m2.remove(("mykey" + i).getBytes());
        }
        var merged = Merger.mergeSegments1(newNullDeleter(), "testdb", List.of(m1, m2), true);
        var itr = merged.lookup(null, null);
        KeyValue kv = itr.next();
        if (kv != null) {
            fail("should be empty, found key="+(new String(kv.key))+" value="+(new String(kv.value)));
        }
    }
}
