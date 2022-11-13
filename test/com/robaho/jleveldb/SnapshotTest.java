package com.robaho.jleveldb;

import com.robaho.jleveldb.exceptions.DatabaseException;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.Arrays;

public class SnapshotTest extends TestCase {
    public void testSnapshotGet() throws DatabaseException, IOException {
        try {
            Database.remove("testdb/mydb");
        } catch(DatabaseException ignore){}

        var db = Database.open("testdb/mydb", new Options(true));

        db.put("mykey".getBytes(), "myvalue".getBytes());

        var s = db.snapshot();

        db.put("mykey1".getBytes(), "myvalue1".getBytes());

        var val= s.get("mykey".getBytes());
        if(!Arrays.equals("myvalue".getBytes(), val)) {
            fail("value does not match");
        }
        val = s.get("keykey1".getBytes());
        if(val != null) {
            fail("should have return nil,KeyNotFound");
        }
        db.close();
    }
}
