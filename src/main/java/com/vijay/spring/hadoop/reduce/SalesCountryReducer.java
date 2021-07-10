package com.vijay.spring.hadoop.reduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class SalesCountryReducer extends Reducer {
    private static final Logger logger = LoggerFactory.getLogger(SalesCountryReducer.class);

    public void reduce(Text t_key, Iterable values, Context context ) throws IOException, InterruptedException {
        Text key = (Text)t_key;
        int frequencyForCountry = 0;
        Iterator valuesIt = values.iterator();
        while (valuesIt.hasNext()) {
            // replace type of value with the actual type of our value
            IntWritable value = (IntWritable) valuesIt.next();
            frequencyForCountry += value.get();
        }
        context.write(key, new IntWritable(frequencyForCountry));

    }
}
