package com.robaho.jleveldb.exceptions;

public class DatabaseAsyncException extends DatabaseException {
    public DatabaseAsyncException(Exception error) {
        super(error);
    }
}
