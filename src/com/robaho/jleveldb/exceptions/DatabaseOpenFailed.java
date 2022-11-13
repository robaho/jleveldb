package com.robaho.jleveldb.exceptions;

import java.io.IOException;

public class DatabaseOpenFailed extends DatabaseException {
    public DatabaseOpenFailed(IOException e) {
        super(e);
    }
}
