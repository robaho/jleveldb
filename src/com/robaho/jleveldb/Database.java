package com.robaho.jleveldb;

import com.robaho.jleveldb.exceptions.*;

import java.io.File;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

class DatabaseState {
    final List<Segment> segments;
    final MemorySegment memory;
    final Segment multi;

    public DatabaseState(List<Segment> segments, MemorySegment memory, MultiSegment multi) {
        this.segments = segments;
        this.memory = memory;
        this.multi = multi;
    }
}
interface Removable {
    void remove() throws IOException;
}
interface Deleter {
    private static Runnable removeSegmentAction(final Removable removable) {
        return () -> {
            try {
//                System.out.println("removing "+removable+" via cleaner");
                removable.remove();
            } catch(Exception e) {
                // TODO notify this failed? database async error callback? might not be able to be
                // removed if file is in use - i.e. being copied during backup by external program
            }
        };
    }
    static Cleaner cleaner = Cleaner.create();
    void scheduleDeletion(List<String> filesToDelete) throws IOException;
    void deleteScheduled() throws IOException;
    static void removeOnFinalize(Segment s,Removable r) {
        cleaner.register(s,removeSegmentAction(r));
    }
}

public class Database {
    private static final int dbMemorySegment = 1024 * 1024;
    private static final int dbMaxSegments = 8;

    static final Object global_lock = new Object();
    final ReentrantLock db_lock = new ReentrantLock(false);
    static final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r,"db executorService");
            t.setDaemon(true);
            return t;
        }
    });

    volatile boolean open;
    final AtomicBoolean inMerge = new AtomicBoolean(false);
    Deleter deleter;
    String path;
    final AtomicLong nextSegID = new AtomicLong();
    LockFile lockFile;
    final WaitGroup wg = new WaitGroup();
    Options options;
    Exception error; // if non-null and async error has occurred
    volatile DatabaseState state;

    public static Database open(String path,Options options) throws DatabaseException {
        Options copy = options.clone();
        synchronized(global_lock){
            try {
                return openImpl(path,copy);
            } catch(DatabaseNotFound e){
                if(options.createIfNeeded)
                    return create(path,copy);
                throw e;
            }
        }
    }

    private static Database create(String path,Options options) throws DatabaseException {
        File dir = new File(path);
        if(!dir.mkdirs())
            throw new DatabaseException("unable to create directories");

        return openImpl(path,options);
    }

    static Database openImpl(String path,Options options) throws DatabaseInvalid, DatabaseNotFound, DatabaseOpenFailed, DatabaseInUseException, DatabaseCorruptedException {
        checkValidDatabase(path);

        String lockFilePath = null;
        try {
            lockFilePath = (new File(path).getCanonicalPath())+"/lockfile";
        } catch (IOException e) {
            throw new DatabaseOpenFailed(e);
        }
        LockFile lockFile = null;
        try {
            lockFile = new LockFile(lockFilePath);
        } catch (IOException e) {
            throw new DatabaseOpenFailed(e);
        }
        if(!lockFile.tryLock())
            throw new DatabaseInUseException();

        Database db = new Database();
        db.path = path;
        db.lockFile = lockFile;
        db.open = true;
        db.options = options;
        db.deleter = new DbDeleter(path);

        try {
            db.deleter.deleteScheduled();
        } catch (IOException e) {
            throw new DatabaseCorruptedException(e);
        }

        List<Segment> segments = null;
        try {
            segments = DiskSegment.loadDiskSegments(path,db.options);
        } catch (IOException e) {
            throw new DatabaseCorruptedException(e);
        }

        long maxSegID = 0;
        for(Segment seg : segments) {
            if(seg.upperID() > maxSegID) {
                maxSegID = seg.upperID();
            }
        }
        db.nextSegID.set(maxSegID);

        MemorySegment memory = new MemorySegment(path,db.nextSegmentID(),db.options);
        MultiSegment multi = new MultiSegment(Segment.copyAndAppend(segments,memory));

        db.state = new DatabaseState(segments,memory,multi);

        if (options.maxMemoryBytes < dbMemorySegment) {
            options.maxMemoryBytes = dbMemorySegment;
        }
        if (options.maxSegments < dbMaxSegments) {
            options.maxSegments = dbMaxSegments;
        }

        if (!options.disableAutoMerge) {
            db.wg.add(1);
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Merger.backgroundMerge(db);
                    } finally {
                        db.wg.done();
                    }
                }
            });
        }

        return db;
    }

    private static void checkValidDatabase(String path) throws DatabaseNotFound,DatabaseInvalid {
        File f = new File(path);
        if(!f.exists()) // empty directories are valid
            throw new DatabaseNotFound();
        if(!f.isDirectory())
            throw new DatabaseInvalid();

        for (File file : f.listFiles()) {
            if(file.getName().equals("lockfile")) {
                continue;
            }
            if(file.getName().equals("deleted")) {
                continue;
            }
            if(file.getName().equals(f.getName()))
                continue;

            if(!file.getName().matches("(log|keys|data)\\..*"))
                throw new DatabaseInvalid();
        }
    }

    public static void remove(String path) throws DatabaseException {
        synchronized (global_lock){
            checkValidDatabase(path);
            String lockFilePath = path+"/lockfile";
            LockFile lockFile = null;
            try {
                lockFile = new LockFile(lockFilePath);
            } catch (IOException e) {
                throw new DatabaseAsyncException(e);
            }
            if(!lockFile.tryLock())
                throw new DatabaseInUseException();
            File dir = new File(path);
            IOUtils.purgeDirectory(dir);
        }
    }

    public void close() throws DatabaseException, IOException {
//        System.out.println("closing, current segments "+stats().numberOfSegments+" to "+options.maxSegments);
        closeWithMerge(options.maxSegments);
    }

    public void closeWithMerge(int numberOfSegments) throws DatabaseException, IOException {
        synchronized(global_lock) {
            lock();
            try {
                if (!open) {
                    throw new DatabaseException("already closed");
                }

                open = false;

                unlock();

                wg.waitEmpty();

                // at this point the background merger should not be running

                state=new DatabaseState(Segment.copyAndAppend(state.segments,state.memory),null,null);

                if (numberOfSegments > 0) {
                    Merger.mergeSegments0(this, numberOfSegments);
                }

                lock();
                for(Segment s : state.segments) {
                    if(s instanceof MemorySegment) {
                        MemorySegment ms = (MemorySegment) s;
                        wg.add(1);
                        executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    DiskIO.writeSegmentToDisk(Database.this.path, ms);
                                } catch(Exception e) {
                                    error = e;
                                }
                                wg.done();
                            }
                        });
                    }
                }
                wg.waitEmpty();

                for (Segment s : state.segments) {
                    s.close();
                }

                deleter.deleteScheduled();

                if(error!=null) {
                    throw new DatabaseAsyncException(error);
                }

            } finally {
                state = new DatabaseState(Collections.EMPTY_LIST,null,null);
                unlock();
                lockFile.unlock();
            }
        }
    }

    public long nextSegmentID() {
        return nextSegID.incrementAndGet();
    }

    DatabaseState getState() {
        return state;
    }

    public byte[] get(byte[] key) throws IOException {
        if(!open)
            throw new DatabaseClosedException();
        if(key.length==0 || key.length>1024)
            throw new IOException("invalid key length");
        byte[] value = getState().multi.get(key);
        if(value!=null && value.length==0)
            return null;
        return value;
    }
    public void put(byte[] key,byte[] value) throws IOException {
        lock();
        try {
            if(!open)
                throw new DatabaseClosedException();
            if(key.length==0 || key.length>1024)
                throw new IOException("invalid key length");

            maybeSwapMemory();
            state.memory.put(key,value);
        } finally {
            unlock();
            maybeMerge();
        }
    }
    public byte[] remove(byte[] key) throws IOException {
        lock();
        try {
            if(!open)
                throw new DatabaseClosedException();
            if(key.length==0 || key.length>1024)
                throw new IOException("invalid key length");
            byte[] value = get(key);
            if(value==null)
                return null;
            maybeSwapMemory();
            state.memory.remove(key);
            return value;
        } finally {
            unlock();
        }
    }

    public void write(WriteBatch batch) throws IOException {
        lock();
        try {
            if(!open) {
                throw new DatabaseClosedException();
            }
            maybeSwapMemory();
            state.memory.write(batch);
        } finally {
            unlock();
            maybeMerge();
        }
    }

    /** creates a read-only snapshot of the database at a moment in time. */
    public Snapshot snapshot() throws IOException {
        lock();
        try {
            if (!open) {
                throw new DatabaseClosedException();
            }
            if(state.memory.size()==0) {
                // memory is unmodified so only the non-memory segments are needed
                var segments = new ArrayList(state.segments);
                return new Snapshot(this,new MultiSegment(segments));
            } else {
                var segments = Segment.copyAndAppend(state.segments,state.memory);
                var memory = new MemorySegment(path,nextSegmentID(),options);
                var multi = new MultiSegment(Segment.copyAndAppend(segments,memory));
                state = new DatabaseState(segments, memory, multi);

                return new Snapshot(this,new MultiSegment(segments));
            }
        } finally {
            unlock();
        }
    }
    void maybeSwapMemory() {
        if(state.memory.size() > options.maxMemoryBytes) {
            var segments = Segment.copyAndAppend(state.segments,state.memory);
            var memory = new MemorySegment(path,nextSegmentID(),options);
            var multi = new MultiSegment(Segment.copyAndAppend(segments,memory));
            state = new DatabaseState(segments, memory, multi);
        }
    }
    void maybeMerge() throws IOException {
        if(options.disableAutoMerge) {
            return;
        }
        var state0 = state;
        if(state0.segments.size() > 2*options.maxSegments) {
            Merger.wakeupMerger();
//            Merger.mergeSegments0(this,options.maxSegments);
//            state0 = state;
//            if(state0.segments.size() > 2*options.maxSegments) {
//                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
//            }
//            // throttle writing to allow background merger to run
        }
    }
    public LookupIterator lookup(byte[] lower,byte[] upper) throws IOException {
        if(!open)
            throw new DatabaseClosedException();
        return snapshot().lookup(lower,upper);
    }

    void lock() {
        db_lock.lock();
    }
    void unlock() {
        db_lock.unlock();
    }

    public Statistics stats() {
        lock();
        try {
            Statistics stats = new Statistics();
            stats.numberOfSegments = state.segments.size();
            return stats;
        } finally {
            unlock();
        }
    }

    class DatabaseLookup implements LookupIterator {
        private LookupIterator itr;
        public DatabaseLookup(LookupIterator itr) {
            this.itr = itr;
        }
        @Override
        public byte[] peekKey() throws IOException {
            throw new IllegalStateException("peekKey should not be called");
        }

        @Override
        public KeyValue next() throws IOException {
            while(true) {
                if (!open) {
                    throw new DatabaseClosedException();
                }
                KeyValue kv = itr.next();
                if(kv!=null && kv.value.length == 0) {
                    continue;
                }
                return kv;
            }
        }
    }

    LookupIterator newDatabaseLookup(LookupIterator itr) {
        return new DatabaseLookup(itr);
    }
}


