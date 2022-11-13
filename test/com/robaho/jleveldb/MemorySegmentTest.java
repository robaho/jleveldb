package com.robaho.jleveldb;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

public class MemorySegmentTest extends TestCase {

    public void testMemorySegment_Put() throws IOException {
        var ms = MemorySegment.newMemoryOnlySegment();
        ms.put("mykey".getBytes(), "myvalue".getBytes());
        var val = ms.get("mykey".getBytes());
        if(Arrays.compare(val, "myvalue".getBytes())!=0) {
            fail();
        }
    }
    public void testMemorySegment_PutOverwrite() throws IOException {
        var ms = MemorySegment.newMemoryOnlySegment();
        ms.put("mykey".getBytes(), "myvalue".getBytes());
        ms.put("mykey".getBytes(), "myvalue2".getBytes());
        var val = ms.get("mykey".getBytes());
        if(Arrays.compare(val, "myvalue2".getBytes())!=0) {
            fail();
        }
    }

    public void testMemorySegment_Remove() throws IOException {
        var ms = MemorySegment.newMemoryOnlySegment();
        ms.put("mykey".getBytes(), "myvalue".getBytes());
        var val = ms.remove("mykey".getBytes());
        if(Arrays.compare(val, "myvalue".getBytes())!=0) {
            fail();
        }
        var itr = ms.lookup(null,null);
        KeyValue kv = itr.next();
        assertTrue(Arrays.compare(kv.key,"mykey".getBytes())==0);
        assertTrue(Arrays.compare(kv.value,new byte[0])==0);
        kv = itr.next();
        assertNull(kv);
    }

    public void testMemorySegment_UserKeyOrder() throws IOException {
        var ms = MemorySegment.newMemoryOnlySegment();
        ms.put("mykey0".getBytes(), "myvalue0".getBytes());
        ms.put("mykey1".getBytes(), "myvalue1".getBytes());
        ms.put("mykey2".getBytes(), "myvalue2".getBytes());
        ms.put("mykey3".getBytes(), "myvalue3".getBytes());

        var itr = ms.lookup(null,null);
        int count=0;
        for(;;count++) {
            var kv = itr.next();
            if(kv==null){
                break;
            }
        }
        assertEquals(4,count);

        itr = ms.lookup("mykey1".getBytes(),null);
        count=0;
        for(;;count++) {
            var kv = itr.next();
            if(kv==null){
                break;
            }
        }
        assertEquals(3,count);
    }

    public void testMemorySegment_Lookup() throws IOException {
        Comparator<byte[]> c = (o1, o2) -> -1 * Arrays.compare(o1,o2);
        Options options = new Options();
        options.userKeyCompare=c;
        var ms = new MemorySegment("",0,options);

        ms.put("mykey1".getBytes(), "myvalue1".getBytes());
        ms.put("mykey2".getBytes(), "myvalue2".getBytes());
        var itr = ms.lookup(null,null);
        KeyValue kv = itr.next();
        if(Arrays.compare(kv.key,"mykey2".getBytes())!=0)
            fail(new String(kv.key));
        if(Arrays.compare(kv.value,"myvalue2".getBytes())!=0)
            fail();
    }

}
