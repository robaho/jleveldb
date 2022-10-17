package com.robaho.jleveldb;

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.ConcurrentSkipListMap;

public class LogFileTest extends TestCase {
    private static void writeLogFile() throws IOException {
        String path = "testdb";
        File dir = new File(path);
        dir.mkdir();
        IOUtils.purgeDirectory(dir);

        var lf = new LogFile(path,0,new Options());
        lf.write("mykey".getBytes(),"myvalue".getBytes());
        lf.startBatch(2);
        lf.write("batchkey1".getBytes(),"batchvalue1".getBytes());
        lf.write("batchkey2".getBytes(),"batchvalue2".getBytes());
        lf.endBatch(2);
        lf.close();
    }
    static void testKeyValue(ConcurrentSkipListMap<byte[],byte[]> s,String key,String value){
        var r = s.get(key.getBytes());
        if(r==null) {
            throw new IllegalStateException("key not found");
        }
        if(Arrays.compare(value.getBytes(),r)!=0) {
            throw new IllegalStateException("values do not match");
        }
    }
    public void testLogFile_Write() throws IOException {
        writeLogFile();
        var s = LogFile.readLogFile("testdb/log.0",new Options());
        testKeyValue(s,"mykey","myvalue");
        testKeyValue(s,"batchkey1","batchvalue1");
        testKeyValue(s,"batchkey2","batchvalue2");
    }


}
