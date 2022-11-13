package com.robaho.jleveldb;

import java.io.IOException;

/** read-only snapshot of the database at a moment in time */
public class Snapshot {
    final Database db;
    final MultiSegment multi;

    Snapshot(Database db,MultiSegment multi) {
        this.db = db;
        this.multi = multi;
    }

    public byte[] get(byte[] key) throws IOException {
        byte[] value = multi.get(key);
        if(value!=null && value.length==0){
            return null;
        }
        return value;
    }
    public LookupIterator lookup(byte[] lower,byte[] higher) throws IOException {
        return db.newDatabaseLookup(multi.lookup(lower,higher));
    }
}


