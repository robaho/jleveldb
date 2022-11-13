package com.robaho.jleveldb.exceptions;

public class DatabaseException extends Exception {
    protected DatabaseException() {
        super();
    }
    protected DatabaseException(Exception e) {
        super(e);
    }
    public DatabaseException(String msg) {
        super(msg);
    }
}
