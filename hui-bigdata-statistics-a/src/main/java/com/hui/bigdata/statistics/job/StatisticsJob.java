package com.hui.bigdata.statistics.job;

import com.hui.bigdata.common.spark.SparkJob;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.beans.Transient;
import java.util.Arrays;

/**
 * <b><code>StatisticsJob</code></b>
 * <p/>
 * Description
 * <p/>
 * <b>Creation Time:</b> 2019/4/18 19:25.
 *
 * @author Hu-Weihui
 * @since hui-bigdata-spark ${PROJECT_VERSION}
 */
@Component
public class StatisticsJob extends SparkJob {
    @Override
    public void execute(JavaSparkContext javaSparkContext) {
        JavaRDD<Integer> parallelize = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4), 3);
        Tuple2<Integer, Integer> reduce = parallelize.mapToPair(x -> new Tuple2<>(x, 1))
                .reduce((x, y) -> getReduce(x, y));
        System.out.println("数组sum:" + reduce._1 + " 计算次数:" + (reduce._2 - 1));
    }

    @Transient
    public Tuple2 getReduce(Tuple2<Integer, Integer> x, Tuple2<Integer, Integer> y) {
        Integer a = x._1();
        Integer b = x._2();
        a += y._1();
        b += y._2();
        return new Tuple2(a, b);
    }
}
