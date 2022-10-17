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
    static void backgroundMerge(Database db) {
        for(;;) {
            db.lock();
            if(db.closing || db.error != null) {
                db.unlock();
                return;
            }

            // the following prevents a Close from occurring while this
            // routine is running

            db.unlock();

            try {
                mergeSegments0(db, Constants.maxSegments);
            } catch (Exception e) {
                e.printStackTrace();
                db.lock();
                db.error = e;
                db.unlock();
            }

            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
    }

    static void mergeSegments0(Database db, int segmentCount) throws IOException {
        if(!db.inMerge.compareAndSet(false,true))
            return;
        try {
            mergeDiskSegments0Exclusive(db,segmentCount);
        } finally {
            db.inMerge.set(false);
        }
    }

    static void mergeDiskSegments0Exclusive(Database db,int segmentCount) throws IOException {
        // must hold the inMerge lock, only a single routine can be here

        int index = 0;

        while(true) {
            List<Segment> segments = db.state.segments;

            if (segments.size() <= segmentCount) {
                return;
            }

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

            if (mergable.size() < 2) {
                if (index == 0)
                    return;
                index = 0;
                continue;
            }

            segments = segments.subList(index, index + mergable.size());

            Segment newseg = mergeSegments1(db.deleter, db.path, segments, index==0);
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
                var newsegments = new ArrayList<Segment>();
                newsegments.addAll(segments.subList(0, index));
                newsegments.add(newseg);
                newsegments.addAll(segments.subList(index + mergable.size(), segments.size()));
                db.state = new DatabaseState(newsegments, db.state.memory, new MultiSegment(Segment.copyAndAppend(newsegments, db.state.memory)));
                index++;
            } finally {
                db.unlock();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
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
