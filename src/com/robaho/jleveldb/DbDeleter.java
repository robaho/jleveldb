package com.robaho.jleveldb;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DbDeleter implements Deleter {
    private final String path;
    private OutputStream file;
    public DbDeleter(String path) {
        this.path = path;
    }
    @Override
    public synchronized void scheduleDeletion(List<String> filesToDelete) throws IOException {
        if(file==null) {
            List<StandardOpenOption> file_options = Arrays.asList(StandardOpenOption.APPEND,StandardOpenOption.WRITE,StandardOpenOption.CREATE,StandardOpenOption.SYNC);
            file = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(Path.of(path+"/deleted"),file_options.toArray(new StandardOpenOption[file_options.size()]))));
        }
        file.write((String.join(",",filesToDelete)+"\n").getBytes());
    }
    @Override
    public synchronized void deleteScheduled() throws IOException {
        if(file!=null) {
            file.close();
            file=null;
        }
        Path filepath = Path.of(path+"/deleted");
        if(!Files.exists(filepath)) {
            return;
        }

        List<String> files = Files.lines(filepath).flatMap(x ->Arrays.stream(x.split(","))).collect(Collectors.toList());
        for(String fname : files) {
            Files.deleteIfExists(Path.of(path,fname));
        }
        Files.deleteIfExists(filepath);
    }
}
