package com.robaho.jleveldb;

public class DatabaseAsyncException extends DatabaseException {
    public DatabaseAsyncException(Exception error) {
        super(error);
    }
}
