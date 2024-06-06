import com.robaho.jleveldb.exceptions.DatabaseException;

import junit.framework.TestCase;

import java.io.IOException;

/** benchmark similar in scope to leveldb db_bench.cc, uses 16 byte keys and 100 byte values */
 public class DbBench extends TestCase {
    DbBenchMain main = new DbBenchMain();

    public void testDbBench() throws IOException, DatabaseException {
        main.testDbBench();;
    }
}
