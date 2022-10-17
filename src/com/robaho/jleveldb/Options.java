package com.robaho.jleveldb;

import java.util.Comparator;

public class Options implements Cloneable {
    public static enum BatchReadMode {
        discardPartial,
        applyPartial,
        returnOpenError
    };
    // If true, then if the database does not exist on Open() it will be created.
    public boolean createIfNeeded;
    // The database segments are periodically merged to enforce MaxSegments.
    // If this is true, the merging only occurs during Close().
    public boolean disableAutoMerge;
    // Maximum number of segments per database which controls the number of open files.
    // If the number of segments exceeds 2x this value, producers are paused while the
    // segments are merged.
    public int maxSegments;
    // Maximum size of memory segment in bytes. Maximum memory usage per database is
    // roughly MaxSegments * MaxMemoryBytes but can be higher based on producer rate.
    public int maxMemoryBytes;
    // Disable flush to disk when writing to increase performance.
    public boolean disableWriteFlush;
    // Force sync to disk when writing. If true, then DisableWriteFlush is ignored.
    public boolean enableSyncWrite;
    // Determines handling of partial batches during Open()
    public BatchReadMode batchReadMode = BatchReadMode.discardPartial;
    // Key comparison function or nil to use standard bytes.Compare
    public Comparator<byte[]> userKeyCompare;

    public Options clone() {
        try {
            return (Options) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException("Options should support clone()");
        }
    }

    public Options() {
    }
    public Options(boolean createIfNeeded) {
        this.createIfNeeded = createIfNeeded;
    }
}
