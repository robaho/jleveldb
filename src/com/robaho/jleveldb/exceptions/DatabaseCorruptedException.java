package com.robaho.jleveldb.exceptions;

import java.io.IOException;

public class DatabaseCorruptedException extends DatabaseException {
    public DatabaseCorruptedException(IOException e) {
        super(e);
    }
}
