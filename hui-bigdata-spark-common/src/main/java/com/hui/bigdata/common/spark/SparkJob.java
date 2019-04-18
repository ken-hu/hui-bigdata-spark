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
    protected SparkJob(){};

    public void execute(JavaSparkContext javaSparkContext, String[] args) {
    };

    public void execute(JavaSparkContext javaSparkContext) {
    };
}
