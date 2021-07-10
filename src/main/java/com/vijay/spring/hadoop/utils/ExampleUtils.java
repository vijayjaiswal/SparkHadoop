package com.vijay.spring.hadoop.utils;

import com.vijay.spring.hadoop.drivers.HDFSFileWriter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ExampleUtils {
    private static final Logger logger = LoggerFactory.getLogger(ExampleUtils.class);
    public static Path getNewFilePath() {
        //Create a path
        String fileName = ExampleUtils.generateFileName() + ".txt";
        Path hdfsWritePath = new Path("/sample/" + fileName);
        return hdfsWritePath;
    }

    public static String generateFileName() {

        int length = 10;
        boolean useLetters = true;
        boolean useNumbers = false;
        String generatedString = RandomStringUtils.random(length, useLetters, useNumbers);

        logger.info(generatedString);
        return generatedString;
    }

    public static String generateContent() throws InterruptedException {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < 50; i++) {
            Thread.currentThread().sleep(1000);
            sb.append("Time Now is: ");
            sb.append(getCurrentTime());
            sb.append(System.lineSeparator());
        }
        logger.info(sb.toString());
        return sb.toString();
    }

    public static String getCurrentTime() {
        Date date = new Date();
        DateTimeZone kolkataTimeZone = DateTimeZone.forID("Asia/Kolkata");
        DateTime dateTimeInKolkata = new DateTime(date, kolkataTimeZone);

        DateTimeFormatter formatter = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss");
        logger.info("dateTimeInKolkata formatted for date: " + formatter.print(dateTimeInKolkata));
        logger.info("dateTimeInKolkata formatted for ISO 8601: " + dateTimeInKolkata);
        return formatter.print(dateTimeInKolkata);
    }
}
