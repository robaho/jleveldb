package com.robaho.jleveldb;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class MultiSegmentIterator implements LookupIterator {
    private final List<LookupIterator> iterators;
    public MultiSegmentIterator(List<LookupIterator> iterators) {
        this.iterators = iterators;
    }

    @Override
    public byte[] peekKey() throws IOException {
        throw new IllegalStateException("peekKey called on multiSegmentIterator");
    }

    @Override
    public KeyValue next() throws IOException {
        int currentIndex = -1;
        byte[] lowest = null;

        // find the lowest next non-deleted key in any of the iterators

        for (int i = iterators.size()-1; i >= 0; i--) {
            var iterator = iterators.get(i);
            byte[] key = iterator.peekKey();

            if(key==null)
                continue;

           if (lowest == null || Arrays.compare(key, lowest)<0) { // needs to be < since we want the latest and we are traversing in reverse order
                lowest = key.clone();
                currentIndex = i;
            }
        }

        if (currentIndex == -1) {
            return null;
        }

        KeyValue kv = iterators.get(currentIndex).next();

        // advance all of the iterators past the current
        for(int i = iterators.size() - 1; i >= 0; i--) {
            if(i == currentIndex) {
                continue;
            }
            var iterator = iterators.get(i);
            while(true) {
                byte[] key = iterator.peekKey();
                if(key==null)
                    break;
                if(Arrays.compare(key,lowest)<=0) { // need to use <= to advance past "old" entries
                    iterator.next();
                } else {
                    break;
                }
            }
        }

        return kv;
    }
}
