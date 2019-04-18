package com.bigdata.spark.sparkstreaming.job;

import com.bigdata.spark.common.SparkJob;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <b><code>WordCountJob</code></b>
 * <p/>
 * Description:
 * <p/>
 * <b>Creation Time:</b> 2018/11/20 21:54.
 *
 * @author Hu Weihui
 */
public class WordCountJob extends SparkJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountJob.class);

    public static void main(String[] args) {

    }

    /**
     * Execute.
     *
     * @param sparkContext the spark context
     * @param args         the args
     * @since hui_project 1.0.0
     */
    @Override
    public void execute(JavaSparkContext sparkContext, String[] args) {
        super.execute(sparkContext, args);
    }

    private void deal(){
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("test");


        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Duration.apply(1000));


    }
}
