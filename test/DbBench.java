import com.robaho.jleveldb.exceptions.DatabaseException;
import com.robaho.jleveldb.exceptions.DatabaseNotFound;
import com.robaho.jleveldb.Options;
import com.robaho.jleveldb.WriteBatch;
import junit.framework.TestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;

import static com.robaho.jleveldb.Database.open;
import static com.robaho.jleveldb.Database.remove;

/** benchmark similar in scope to leveldb db_bench.cc, uses 16 byte keys and 100 byte values */
 public class DbBench extends TestCase {
    static final int nr = 1000000;
    static final int vSize = 100;
    static final int kSize = 16;
    static final int batchSize = 1000;
    static final String dbname = "testdb/mydb";

    byte[] value;

    public void testDbBench() throws IOException, DatabaseException {
        value = new byte[vSize];
        Random r = new Random(System.currentTimeMillis());
        r.nextBytes(value);

        _testWrite(false,true);
        _testWrite(true,true);
        _testBatch();
        _testWrite(false,false);
        _testRandom();
        _testRead();
        _testCompact();
        _testRandom();
        _testRead();
    }

    private void _testWrite(boolean sync,boolean remove) throws DatabaseException, IOException {
        if(remove) {
            try {
                remove(dbname);
            } catch(DatabaseNotFound ignore){}
        }

        var n = nr;
        if(sync) {
            n = n / 100;
        }
        Options options = new Options(true);
        options.enableSyncWrite = sync;
        var db = open(dbname,options);
        long start = System.currentTimeMillis();
        for(int i=0;i<n;i++) {
            var key = String.format("%0"+kSize+"d",i).getBytes();
            db.put(key,value);
        }
        long end = System.currentTimeMillis();
        long duration = end-start;
        String mode = (sync ? "sync" : "no-sync") + (remove ? "" : " overwrite");
        System.out.printf("write %s time %d records = %d ms, usec per op %.3f\n",mode,n,duration,(duration*1000.0)/n);
        start = System.currentTimeMillis();
        db.closeWithMerge(0);
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
                var key = String.format("%0"+kSize+"d",i+j).getBytes();
                wb.put(key,value);
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
    private void _testCompact() throws DatabaseException, IOException {
        var db = open(dbname,new Options());
        long start = System.currentTimeMillis();
        db.closeWithMerge(1);
        long end = System.currentTimeMillis();
        long duration = end-start;
        System.out.println("compact time "+duration+" ms");
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
        System.out.printf("seq read time %d ms, usec per op %.3f\n",duration,(duration*1000.0)/nr);
        db.close();
    }

    private static void _testRandom() throws IOException, DatabaseException {
        var db = open(dbname,new Options());

        var start = System.currentTimeMillis();

        var r = new Random();

        for(int i = 0; i < nr; i++) {
            int index = r.nextInt(nr);
            var key = String.format("%0"+kSize+"d",index).getBytes();
            if(db.get(key)==null) {
                throw new IllegalStateException("key not found "+new String(key));
            }
        }

        var end = System.currentTimeMillis();
        var duration = end-start;

        System.out.println("random read time "+ duration+ "ms, usec per op "+ (duration*1000.0)/nr);

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
