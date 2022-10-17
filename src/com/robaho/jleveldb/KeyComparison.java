package com.robaho.jleveldb;

import java.util.Arrays;
import java.util.Comparator;

public interface KeyComparison {
    static Comparator<byte[]> newKeyCompare(Options options) {
        if(options.userKeyCompare!=null) {
            return options.userKeyCompare;
        } else {
            return new Comparator<byte[]>() {
                @Override
                public int compare(byte[] a, byte[] b) {
                    return Arrays.compare(a,b);
                }
            };
        }
    }
}
