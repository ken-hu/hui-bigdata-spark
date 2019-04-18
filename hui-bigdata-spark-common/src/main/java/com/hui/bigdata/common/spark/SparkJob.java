package com.hui.bigdata.common.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * <b><code>SparkJob</code></b>
 * <p/>
 * Description
 * <p/>
 * <b>Creation Time:</b> 2019/4/12 10:01.
 *
 * @author Hu-Weihui
 * @since hui-bigdata-springboot ${PROJECT_VERSION}
 */
@Slf4j
public abstract class SparkJob implements Serializable {
    /**
     * Instantiates a new Spark job.
     */
    protected SparkJob(){};

    /**
     * 带参数
     *
     * @param javaSparkContext the java spark context
     * @param args             the args
     * @author HuWeihui
     * @since hui_project v1
     */
    public void execute(JavaSparkContext javaSparkContext, String[] args) {
    };

    /**
     * 不带参数
     *
     * @param javaSparkContext the java spark context
     * @author HuWeihui
     * @since hui_project v1
     */
    public void execute(JavaSparkContext javaSparkContext) {
    };
}
