package com.robaho.jleveldb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class Utils {
    public static String trimSuffix(String s, String suffix) {
        if(!s.endsWith(suffix))
            return s;
        int index = s.lastIndexOf(suffix);
        if(index<0)
            return s;
        return s.substring(0,index);
    }
    public static String trimPrefix(String s, String prefix) {
        if(!s.startsWith(prefix))
            return s;
        return s.substring(prefix.length());
    }

    public static void removeFileIfExists(String path, String filename) throws IOException {
        File file = new File(path,filename);
        if(file.exists()) {
            if(!file.delete()) {
                throw new IOException("unable to delete "+file);
            }
        }
    }
    static long getSegmentID(String path) {
        File file = new File(path);
        String base = file.getName();
        String[] segs = base.split("\\.");
        long id = Long.parseLong(segs[1]);
        return id;
    }
    static long[] getSegmentIDs(String name) {
        long[] ids = new long[2];
        String[] segs = name.split("\\.");
        ids[0] = Long.parseLong(segs[1]);
        ids[1] = Long.parseLong(segs[2]);
        return ids;
    }

    public static String getFileName(String filepath) {
        return Path.of(filepath).getFileName().toString();
    }
}
