package com.robaho.jleveldb;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

class LogFile {
    Path filepath;
    private DataOutputStream w;
    private long id;
    private boolean inBatch;
    private boolean syncWrite;
    private boolean disableFlush;

    LogFile(String path,long id,Options options) throws IOException {
        filepath = Path.of(path+"/log."+id);
        List<StandardOpenOption> file_options = new ArrayList<>(Arrays.asList(StandardOpenOption.TRUNCATE_EXISTING,StandardOpenOption.WRITE,StandardOpenOption.CREATE));
        if(options.enableSyncWrite){
            file_options.add(StandardOpenOption.SYNC);
        }
        if(!options.enableSyncWrite && options.disableWriteFlush) {
            disableFlush = true;
        }
        
        w = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(filepath,file_options.toArray(new StandardOpenOption[file_options.size()]))));
    }
    public void startBatch(int len) throws IOException {
        inBatch = true;
        w.writeInt(-len);
    }
    public void endBatch(int len) throws IOException {
        inBatch = false;
        w.writeInt(-len);
        w.flush();
    }
    public void write(byte[] key,byte[] value) throws IOException {
        w.writeInt(key.length);
        w.write(key);
        w.writeInt(value.length);
        w.write(value);
        if (!inBatch && !disableFlush) {
            w.flush();
        }
    }
    public void close() throws IOException {
        w.flush();
        w.close();
    }
    public void remove() throws IOException {
        Files.delete(filepath);
    }

    static class LogFileReader {
        private ConcurrentSkipListMap<byte[],byte[]> list;
        private final DataInputStream is;
        private Options options;
        private LogFileReader(String path,Options options) throws FileNotFoundException {
            list = new ConcurrentSkipListMap(KeyComparison.newKeyCompare(options));
            this.options = options;
            is = new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
        }
        private KeyValue readEntry(int keyLen) throws IOException {
            byte[] key = new byte[keyLen];
            is.read(key);
            int valueLen = is.readInt();
            byte[] value = new byte[valueLen];
            is.read(value);
            return new KeyValue(key,value);
        }
        private ConcurrentSkipListMap<byte[],byte[]> readLog() throws IOException {
            try (is) {
                while (true) {
                    int len;
                    try {
                        len = is.readInt();
                    } catch (EOFException e) {
                        return list;
                    }
                    if(len<0) {
                        readBatch(len);
                    } else {
                        var kv = readEntry(len);
                        list.put(kv.key,kv.value);
                    }
                }
            }
        }
        private void readBatch(int len) throws IOException {
            List<KeyValue> entries = new LinkedList<>();
            try {
                for (int i = 0; i < (len * -1); i++) {
                    int keyLen = is.readInt();
                    entries.add(readEntry(keyLen));
                }
                int eob = is.readInt();
                if(eob!=len) {
                    throw new IOException("batch len does not match");
                }
            } catch(IOException e) {
                if(options.batchReadMode== Options.BatchReadMode.returnOpenError)
                    throw e;
                if(options.batchReadMode== Options.BatchReadMode.discardPartial) {
                    return;
                }
            }
            for(KeyValue kv : entries) {
                list.put(kv.key,kv.value);
            }
        }
        static ConcurrentSkipListMap<byte[],byte[]> readLogFile(String path,Options options) throws IOException {
            LogFileReader r = new LogFileReader(path,options);
            return r.readLog();
        }
    }

    static ConcurrentSkipListMap<byte[],byte[]> readLogFile(String path,Options options) throws IOException {
        return LogFileReader.readLogFile(path,options);
    }
}
