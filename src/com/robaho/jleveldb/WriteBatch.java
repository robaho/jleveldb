package com.robaho.jleveldb;

import java.util.ArrayList;
import java.util.List;

public class WriteBatch {
    List<KeyValue> entries = new ArrayList<>();
    public void put(byte[] key,byte[] value) {
        entries.add(new KeyValue(key,value));
    }
    public void remove(byte[] key){
        entries.add(new KeyValue(key));
    }
}
