package com.robaho.jleveldb;

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class DiskSegmentTest extends TestCase {
    public void testDiskSegment() throws IOException {
        File dir = new File("testdb");
        IOUtils.purgeDirectory(dir);
        dir.mkdirs();
        
        var m = MemorySegment.newMemoryOnlySegment();

        m.put("mykey".getBytes(), "myvalue".getBytes());
        m.put("mykey2".getBytes(), "myvalue2".getBytes());
        m.put("mykey3".getBytes(), "myvalue3".getBytes());

        var itr = m.lookup(null, null);

        var ds = DiskIO.writeAndLoadSegment("testdb/keys.0.0", "testdb/data.0.0", itr, false);

        itr = ds.lookup(null,null);
        int count =0;
        while(true) {
            if(itr.next()==null)
                break;
            count++;
        }
        assertEquals(3,count);

        var value = ds.get("mykey".getBytes());
        if (!Arrays.equals(value,"myvalue".getBytes())) {
            fail("incorrect values");
        }
        value = ds.get("mykey2".getBytes());
        if (!Arrays.equals(value,"myvalue2".getBytes())) {
            fail("incorrect values");
        }
        value = ds.get("mykey3".getBytes());
        if(!Arrays.equals(value,"myvalue3".getBytes())) {
            fail("incorrect values");
        }
        value = ds.get("mykey4".getBytes());
        if(value!=null) {
            fail("key should not be found");
        }
    }

    public void testLargeDiskSegment() throws IOException {
        File dir = new File("testdb");
        IOUtils.purgeDirectory(dir);
        dir.mkdirs();

        var m = MemorySegment.newMemoryOnlySegment();
        for(int i=0;i<1000000;i++) {
            m.put(("mykey"+i).getBytes(),("myvalue"+i).getBytes());
        }

        var itr = m.lookup(null, null);

        var ds = DiskIO.writeAndLoadSegment("testdb/keys.0.0", "testdb/data.0.0", itr, false);

        itr = ds.lookup(null,null);
        int count =0;
        while(true) {
            if(itr.next()==null)
                break;
            count++;
        }
        assertEquals(1000000,count);

        var value = ds.get("mykey1".getBytes());
        if (!Arrays.equals(value,"myvalue1".getBytes())) {
            fail("incorrect values");
        }
    }
    public void testEmptySegment() throws IOException {
        File dir = new File("testdb");
        IOUtils.purgeDirectory(dir);
        dir.mkdirs();
        var m = MemorySegment.newMemoryOnlySegment();
        m.put("mykey".getBytes(),"myvalue".getBytes());
        m.remove("mykey".getBytes());
        var itr = m.lookup(null,null);
        var ds = DiskIO.writeAndLoadSegment("testdb/keys.0.0","testdb/data.0.0",itr, false);
        itr = ds.lookup(null,null);
        int count =0;
        while(true) {
            if(itr.next()==null)
                break;
            count++;
        }
        assertEquals(1,count);
    }
    public void testEmptySegmentWithPurge() throws IOException {
        File dir = new File("testdb");
        IOUtils.purgeDirectory(dir);
        dir.mkdirs();
        var m = MemorySegment.newMemoryOnlySegment();
        m.put("mykey".getBytes(),"myvalue".getBytes());
        m.remove("mykey".getBytes());
        var itr = m.lookup(null,null);
        var ds = DiskIO.writeAndLoadSegment("testdb/keys.0.0","testdb/data.0.0",itr, true);
        itr = ds.lookup(null,null);
        int count =0;
        while(true) {
            if(itr.next()==null)
                break;
            count++;
        }
        assertEquals(0,count);
    }
}
