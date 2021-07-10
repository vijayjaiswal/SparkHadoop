package com.vijay.spring.hadoop.reduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TextReduce extends Reducer {
    private static final Logger logger = LoggerFactory.getLogger(TextReduce.class);

    protected void reduce(Text key, Iterable values, Context context)throws IOException, InterruptedException {

        int sum = 0;
        Iterator valuesIt = values.iterator();
        logger.info("valuesIt: "+valuesIt);
        while(valuesIt.hasNext()){
            int temp =(Integer) valuesIt.next();
            logger.info("temp: "+temp);
            sum = sum + temp;
        }

        context.write(key, new IntWritable(sum));
    }
}
