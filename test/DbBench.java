import com.robaho.jleveldb.DatabaseException;
import com.robaho.jleveldb.Options;
import com.robaho.jleveldb.WriteBatch;
import junit.framework.TestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.stream.Collectors;

import static com.robaho.jleveldb.Database.open;
import static com.robaho.jleveldb.Database.remove;

/** benchmark similar in scope to leveldb db_bench.cc, uses 16 byte keys and 100 byte values */
 public class DbBench extends TestCase {
    static final int nr = 10000000;
    static final int vSize = 100;
    static final int kSize = 16;
    static final int batchSize = 1000;
    static final String dbname = "testdb/mydb";

    byte[] value;

    public void testDbBench() throws IOException, DatabaseException {
        value = new byte[vSize];
        Random r = new Random(System.currentTimeMillis());
        r.nextBytes(value);

        _testWrite(false);
        _testBatch();
        _testWrite(true);
        _testRead();

        var db = open(dbname, new Options());
        long start = System.currentTimeMillis();
        db.closeWithMerge(1);
        long end = System.currentTimeMillis();
        long duration = end-start;
        System.out.println("close with merge 1 time "+ duration+" ms");
        _testRead();
    }

    private void _testWrite(boolean sync) throws DatabaseException, IOException {
        remove(dbname);
        Options options = new Options(true);
        options.enableSyncWrite = sync;
        var db = open(dbname,options);
        long start = System.currentTimeMillis();
        for(int i=0;i<nr;i++) {
            var key = new byte[kSize];
            var keyS = String.format("%07d",i).getBytes();
            System.arraycopy(keyS,0,key,0,keyS.length);
            db.put(key,value);
        }
        long end = System.currentTimeMillis();
        long duration = end-start;
        String mode = sync ? "sync" : "no-sync";
        System.out.printf("insert %s time %d records = %d ms, usec per op %.3f\n",mode,nr,duration,(duration*1000.0)/nr);
        start = System.currentTimeMillis();
        db.close();
        end = System.currentTimeMillis();
        duration = end-start;
        System.out.println("close time "+duration+" ms");
        System.out.println("database size "+dbsize(dbname));
    }
    private void _testBatch() throws DatabaseException, IOException {
        remove(dbname);
        Options options = new Options(true);
        options.maxSegments = 64;
        var db = open(dbname,options);
        long start = System.currentTimeMillis();
        for(int i=0;i<nr;) {
            var wb = new WriteBatch();
            for(int j=0;j<batchSize;j++){
                String key = String.format("%07d",i+j);
                wb.put(key.getBytes(),value);
            }
            db.write(wb);
            i += batchSize;
        }
        long end = System.currentTimeMillis();
        long duration = end-start;
        System.out.printf("batch insert time %d records = %d ms, usec per op %.3f\n",nr,duration,(duration*1000.0)/nr);
        start = System.currentTimeMillis();
        db.close();
        end = System.currentTimeMillis();
        duration = end-start;
        System.out.println("close time "+duration+" ms");
        System.out.println("database size "+dbsize(dbname));


    }
    private void _testRead() throws DatabaseException, IOException {
        var db = open(dbname,new Options());
        long start = System.currentTimeMillis();
        var itr = db.lookup(null,null);
        int count = 0;
        while(true) {
            if(itr.next()==null) {
                break;
            }
            count++;
        }
        if(count!=nr) {
            throw new IllegalStateException("incorrect count "+nr+" != "+count);
        }
        long end = System.currentTimeMillis();
        long duration = end-start;
        System.out.printf("scan time %d ms, usec per op %.3f\n",duration,(duration*1000.0)/nr);
        db.close();
    }
    private static String dbsize(String dbname) throws IOException {
        long size = 0;
        for(Path file : Files.list(Path.of(dbname)).collect(Collectors.toList())) {
            size += Files.size(file);
        }
        return String.format("%.1fM",size/(1024.0*1024.0));
    }
}
