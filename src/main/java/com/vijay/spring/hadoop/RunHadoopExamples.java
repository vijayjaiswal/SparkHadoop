package com.vijay.spring.hadoop;

import com.vijay.spring.hadoop.drivers.HDFSFileWriter;
import com.vijay.spring.hadoop.utils.ExampleUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunHadoopExamples {
    private static final Logger logger = LoggerFactory.getLogger(RunHadoopExamples.class);
    public static void main(String[] args){
        HDFSFileWriter hdfsFileWriter =new HDFSFileWriter();
        try{
            Path path =ExampleUtils.getNewFilePath();
            logger.info("Writing to file: "+path.toUri());
            hdfsFileWriter.writeFileToHDFS(path);
            logger.info("Reading from file: "+path.toUri());
            hdfsFileWriter.readFileFromHDFS(path);
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}
