package com.vijay.spring.hadoop.drivers;

import com.vijay.spring.hadoop.map.SalesMapper;
import com.vijay.spring.hadoop.reduce.SalesCountryReducer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.FileSystems;

public class SalesCountryDriver extends Configured implements Tool {
    private static final Logger logger = LoggerFactory.getLogger(SalesCountryDriver.class);
    public static void main(String[] args) throws Exception {


        int exitCode = ToolRunner.run(new SalesCountryDriver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }
        java.nio.file.Path path = FileSystems.getDefault().getPath("").toAbsolutePath();
        logger.info("path "+path.toString());
        File folderToBeDeleted=new File(path.toString()+"/"+args[1]);
        logger.info("Deleting "+folderToBeDeleted.getAbsolutePath());
        FileUtils.deleteDirectory(folderToBeDeleted);

        Job job = Job.getInstance();
        job.setJarByClass(MapReduceExample.class);
        job.setJobName("SalePerCountry");


        // Specify data type of output key and value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // Specify names of Mapper and Reducer Class
        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesCountryReducer.class);
        // Set input and output directories
        // arg[0] = name of input directory on HDFS, and
        // arg[1] = name of output directory to be created
        // to store the output file.
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        int returnValue = job.waitForCompletion(true) ? 0:1;

        if(job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if(!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }

        return returnValue;
    }
}

