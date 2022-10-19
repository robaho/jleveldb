import com.robaho.jleveldb.Database;
import com.robaho.jleveldb.DatabaseException;
import com.robaho.jleveldb.LookupIterator;
import com.robaho.jleveldb.Options;

import java.io.IOException;
import java.util.Random;

public class PerformanceRandomRead {
    static final int nr = 10000000;

    public static void main(String[] args) throws DatabaseException, IOException {
        Database db = Database.open("testdb/mydb", new Options(false));
        System.out.println("number of segments "+db.stats().numberOfSegments);

        for(int i=0;i<10;i++) {
            testRandom(db);
        }

        db.close();
    }

    private static void testRandom(Database db) throws IOException {

        var start = System.currentTimeMillis();

        var r = new Random();

        for(int i = 0; i < nr/10; i++) {
            int index = r.nextInt(nr / 10);
            if(db.get(String.format("mykey%7d", index).getBytes())==null) {
                throw new IllegalStateException("key not found");
            }
        }

        var end = System.currentTimeMillis();
        var duration = end-start;

        System.out.println("random time "+ duration+ "ms, usec per op "+ (duration*1000.0)/(nr/10));

    }



}
