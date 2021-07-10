package com.vijay.spring.hadoop.drivers;

import com.vijay.spring.hadoop.map.TextMap;
import com.vijay.spring.hadoop.reduce.TextReduce;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapReduceExample extends Configured implements Tool{
    private static final Logger logger = LoggerFactory.getLogger(MapReduceExample.class);
    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new MapReduceExample(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }

        Job job = Job.getInstance();
        job.setJarByClass(MapReduceExample.class);
        job.setJobName("MapReduceWordCounter");

        // Deleting existing Output folder
        logger.info("Deleting existing Output folder: "+args[1]);
        //FileUtils.deleteDirectory(new File(args[1]));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(TextMap.class);
        job.setReducerClass(TextReduce.class);

        int returnValue = job.waitForCompletion(true) ? 0:1;

        if(job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if(!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }

        return returnValue;
    }
}
