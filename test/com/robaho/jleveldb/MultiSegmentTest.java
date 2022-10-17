package com.robaho.jleveldb;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.List;

public class MultiSegmentTest extends TestCase {
    public void testMultiSegment() throws IOException {
        var m1 = MemorySegment.newMemoryOnlySegment();
        for(int i=0;i<100000;i++) {
            m1.put(("mykey"+i).getBytes(),("myvalue"+i).getBytes());
        }
        var m2 = MemorySegment.newMemoryOnlySegment();
        for(int i=10000;i<150000;i++) {
            m1.put(("mykey"+i).getBytes(),("myvalue"+i).getBytes());
        }
        var ms = new MultiSegment(List.of(m1,m2));
        var itr = ms.lookup(null,null);
        int count =0;
        while(true) {
            KeyValue kv = itr.next();
            if(kv==null)
                break;
            count++;
        }
        assertEquals(150000,count);
    }
    public void testMultiSegment2() throws IOException {
        var m1 = MemorySegment.newMemoryOnlySegment();
        for(int i=0;i<1;i++) {
            m1.put(("mykey"+i).getBytes(),("myvalue"+i).getBytes());
        }
        var m2 = MemorySegment.newMemoryOnlySegment();
        for(int i=1;i<150000;i++) {
            m1.put(("mykey"+i).getBytes(),("myvalue"+i).getBytes());
        }
        var ms = new MultiSegment(List.of(m1,m2));
        var itr = ms.lookup(null,null);
        int count =0;
        while(true) {
            KeyValue kv = itr.next();
            if(kv==null)
                break;
            count++;
        }
        assertEquals(150000,count);
    }
}
