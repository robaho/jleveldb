import com.robaho.jleveldb.Database;
import com.robaho.jleveldb.exceptions.DatabaseException;
import com.robaho.jleveldb.LookupIterator;
import com.robaho.jleveldb.Options;

import java.io.IOException;

public class PerformanceRead {
    static final int nr = 10000000;

    public static void main(String[] args) throws DatabaseException, IOException {
        Database db = Database.open("testdb/mydb", new Options(false));
        System.out.println("number of segments "+db.stats().numberOfSegments);

        for(int i=0;i<10;i++) {
            testRead(db);
        }

        db.close();
    }

    private static void testRead(Database db) throws IOException, DatabaseException {

        var start = System.currentTimeMillis();
        LookupIterator itr = db.lookup(null,null);
        int count = 0;
        while(true) {
            if(itr.next()==null)
                break;
            count++;
            if(count > nr+1000) {
                throw new IllegalStateException("incorrect, finding too many records... aborting");
            }
        }
        if(count != nr) {
            throw new IllegalStateException("incorrect count != "+ nr + ", count is "+ count);
        }
        var end = System.currentTimeMillis();
        var duration = end-start;

        System.out.println("scan time "+ duration+ "ms, usec per op "+ (duration*1000.0)/nr);
    }


}
