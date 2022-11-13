package com.robaho.jleveldb;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
    public void testDiskSegmentKeyFile() throws IOException {
        File dir = new File("testdb");
        IOUtils.purgeDirectory(dir);
        dir.mkdirs();

        var m = MemorySegment.newMemoryOnlySegment();

        m.put("mykey".getBytes(), "myvalue".getBytes());

        var itr = m.lookup(null, null);

        var ds = DiskIO.writeAndLoadSegment("testdb/keys.0.0", "testdb/data.0.0", itr, false);

        FileInputStream fis = new FileInputStream("testdb/keys.0.0");
        byte[] data = fis.readNBytes(2+5);
        // data is keyLength (little endian) followed by keyLength bytes of data
        assertEquals(5,data[0]);
        assertEquals(0,data[1]);
        assertEquals('m',data[2]);
        assertEquals('y',data[3]);
        assertEquals('k',data[4]);
        assertEquals('e',data[5]);
        assertEquals('y',data[6]);
        fis.close();

        assertTrue(Arrays.compare(data,2,7,ds.keyIndex.get(0),0,5)==0);
    }

    public void testKeyIndex() throws IOException {
        File dir = new File("testdb");
        IOUtils.purgeDirectory(dir);
        dir.mkdirs();

        var m = MemorySegment.newMemoryOnlySegment();

        m.put("mykey".getBytes(), "myvalue".getBytes());

        var itr = m.lookup(null, null);

        var ds = DiskIO.writeAndLoadSegment("testdb/keys.0.0", "testdb/data.0.0", itr, false);
        var ds0 = new DiskSegment("testdb/keys.0.0", "testdb/data.0.0",null);

        assertTrue(Arrays.compare("mykey".getBytes(),ds0.keyIndex.get(0))==0);

    }

    public void testMemorySegmentToDiskSegment() throws IOException {
        File dir = new File("testdb");
        IOUtils.purgeDirectory(dir);
        dir.mkdirs();

        var m = MemorySegment.newMemoryOnlySegment();

        m.put("mykey".getBytes(), "myvalue".getBytes());
        m.put("mykey2".getBytes(), "myvalue2".getBytes());
        m.put("mykey3".getBytes(), "myvalue3".getBytes());

        var itr = m.lookup(null, null);

        var ds = DiskIO.writeSegmentToDisk(dir.getPath(), m);

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

    public void testDiskSegmentOverwrite() throws IOException {
        File dir = new File("testdb");
        IOUtils.purgeDirectory(dir);
        dir.mkdirs();

        var m = MemorySegment.newMemoryOnlySegment();

        m.put("mykey".getBytes(), "myvalue".getBytes());
        m.put("mykey2".getBytes(), "myvalue2".getBytes());
        m.put("mykey3".getBytes(), "myvalue3".getBytes());

        var itr = m.lookup(null, null);

        var ds1 = DiskIO.writeAndLoadSegment("testdb/keys.0.0", "testdb/data.0.0", itr, false);
        var ds2 = DiskIO.writeAndLoadSegment("testdb/keys.1.1", "testdb/data.1.1", itr, false);

        var ms = new MultiSegment(List.of(ds1,ds2));
        var itr0 = ms.lookup(null,null);
        int count =0;
        while(true) {
            if(itr0.next()==null)
                break;
            count++;
        }
        assertEquals(3,count);
        IOUtils.purgeDirectory(dir);
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
        value = ds.get("mykey500000".getBytes());
        if (!Arrays.equals(value,"myvalue500000".getBytes())) {
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
