package com.robaho.jleveldb;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static com.robaho.jleveldb.DiskIO.writeAndLoadSegment;

class Merger {
    static Thread mergerThread;
    static void backgroundMerge(Database db) {
        mergerThread = Thread.currentThread();

        for(;;) {
            db.lock();
            if(!db.open || db.error != null) {
                db.unlock();
                return;
            }

            // the following prevents a Close from occurring while this
            // routine is running

            db.unlock();

            try {
                mergeSegments0(db, Constants.maxSegments,true);
            } catch (Exception e) {
                db.lock();
                db.error = e;
                db.unlock();
            }

            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
    }

    static void wakeupMerger() {
        LockSupport.unpark(mergerThread);
    }

    static void mergeSegments0(Database db, int segmentCount, boolean throttle) throws IOException {
        if(!db.inMerge.compareAndSet(false,true))
            return;
        try {
            mergeDiskSegments0Exclusive(db,segmentCount,throttle);
        } finally {
            db.inMerge.set(false);
        }
    }

    static void mergeDiskSegments0Exclusive(Database db,int segmentCount,boolean throttle) throws IOException {
        // must hold the inMerge lock, only a single routine can be here

        while(true) {
            List<Segment> segments = db.state.segments;

            if (segments.size() <= segmentCount) {
                return;
            }

//            System.out.println("======= "+segments.size()+" segments");
//            for(Segment s : segments) {
//                System.out.println(""+s.lowerID()+","+s.upperID()+" = "+(s.size()/(1024*1024))+"M");
//            }

            int smallest = 0;
            for(int i=1;i<segments.size();i++) {
                if(segments.get(i).size() < segments.get(smallest).size()) {
                    smallest=i;
                }
            }
            if(smallest>0 && smallest==segments.size()-1) {
                smallest--;
            }

            int index = smallest;

            int maxMergeSize = segments.size() / 2;
            if (maxMergeSize < 4) {
                maxMergeSize = 4;
            }

            // ensure that only valid disk segments are merged

            var mergable = new ArrayList<Segment>();

            for (Segment s : segments.subList(index, segments.size())) {
                mergable.add(s);
                if (mergable.size() == maxMergeSize)
                    break;
            }

            segments = segments.subList(index, index + mergable.size());

            Segment newSegment = mergeSegments1(db.deleter, db.path, segments, index==0);
            db.lock();
            try {
                segments = db.state.segments;
                for (int i = 0; i < mergable.size(); i++) {
                    if (mergable.get(i) != segments.get(i + index)) {
                        throw new IllegalStateException("unexpected segment change");
                    }
                }
                for (Segment s : mergable) {
                    s.removeOnFinalize();
                }
                var newSegments = new ArrayList<Segment>();
                newSegments.addAll(segments.subList(0, index));
                newSegments.add(newSegment);
                newSegments.addAll(segments.subList(index + mergable.size(), segments.size()));
                db.state = new DatabaseState(newSegments, db.state.memory, new MultiSegment(Segment.copyAndAppend(newSegments, db.state.memory)));
            } finally {
                db.unlock();
                if(throttle) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                }
            }
        }
    }

    static Segment mergeSegments1(Deleter deleter,String dbpath,List<Segment> segments,boolean removeDeleted) throws IOException {
        long lowerId = segments.get(0).lowerID();
        long upperId = segments.get(segments.size()-1).upperID();

        String keyFilename = String.format("%s/keys.%d.%d",dbpath,lowerId,upperId);
        String dataFilename = String.format("%s/data.%d.%d",dbpath,lowerId,upperId);

        List<String> files = new LinkedList<>();
        for(Segment s : segments) {
            files.addAll(s.files());
        }
        MultiSegment ms = new MultiSegment(segments);
        LookupIterator itr = ms.lookup(null,null);
        Segment seg = writeAndLoadSegment(keyFilename,dataFilename,itr,removeDeleted);
        deleter.scheduleDeletion(files);
        return seg;
    }
}
