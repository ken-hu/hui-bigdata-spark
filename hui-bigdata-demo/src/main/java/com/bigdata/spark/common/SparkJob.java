package com.bigdata.spark.common;

import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <b><code>SparkJob</code></b>
 * <p/>
 * Description:
 * <p/>
 * <b>Creation Time:</b> 2018/11/11 17:39.
 *
 * @author Hu Weihui
 */
public class SparkJob {
    /**
     * The constant LOGGER.
     *
     * @since hui_project 1.0.0
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkJob.class);

    /**
     * The constant serialVersionUID.
     *
     * @since hui_project 1.0.0
     */
    private static final long serialVersionUID = 771902776566370732L;

    /**
     * Instantiates a new Spark job.
     */
    protected SparkJob(){}

    /**
     * Execute.
     *
     * @param sparkContext the spark context
     * @param args         the args
     * @since hui_project 1.0.0
     */
    public void execute(JavaSparkContext sparkContext, String[] args) {
    }

    /**
     * Execute.
     *
     * @param sparkContext the spark context
     * @since hui_project 1.0.0
     */
    public void execute(JavaSparkContext sparkContext) {

    }
}
