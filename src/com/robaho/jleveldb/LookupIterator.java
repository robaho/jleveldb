package com.robaho.jleveldb;

import java.io.IOException;

interface LookupIteratorInternal {
    /**
     * @return null if there are no more keys
     * @throws IOException
     */
    byte[] peekKey() throws IOException;
}
public interface LookupIterator extends LookupIteratorInternal {
    /**
     * @return null if there are no more keys, KeyValue.value is KeyValue.EMPTY if removed.
     * @throws IOException
     */
    KeyValue next() throws IOException;
    static LookupIterator EMPTY = new LookupIterator() {
        @Override
        public KeyValue next() throws IOException {
            return null;
        }
        @Override
        public byte[] peekKey() throws IOException {
            return null;
        }
    };
}

