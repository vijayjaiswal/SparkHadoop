package com.vijay.spring.hadoop.drivers;

import com.vijay.spring.hadoop.reduce.TextReduce;
import com.vijay.spring.hadoop.utils.ExampleUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Date;

public class HDFSFileWriter {
    private static final Logger logger = LoggerFactory.getLogger(HDFSFileWriter.class);

    public void writeFileToHDFS(Path hdfsWritePath) throws IOException, InterruptedException {
        FileSystem fileSystem = getHDFSFileSystem();
        FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath, true);

        BufferedWriter bufferedWriter = new BufferedWriter(new
                OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
        bufferedWriter.write(ExampleUtils.generateContent());
        bufferedWriter.newLine();
        bufferedWriter.close();
        fileSystem.close();
    }

    public void readFileFromHDFS(Path inFilePath) throws IOException {
        FileSystem fs = getHDFSFileSystem();
        FSDataInputStream in = fs.open(inFilePath);
        OutputStream out = System.out;
        byte buffer[] = new byte[256];
        int bytesRead = 0;
        while ((bytesRead = in.read(buffer)) > 0) {
            out.write(buffer, 0, bytesRead);
        }
        in.close();
        out.close();
    }

    private FileSystem getHDFSFileSystem() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        return fileSystem;
    }



}
