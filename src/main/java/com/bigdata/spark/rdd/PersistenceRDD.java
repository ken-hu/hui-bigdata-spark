package com.bigdata.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * <b><code>PersistenceRDD</code></b>
 * <p/>
 * Description:
 * <p/>
 * <b>Creation Time:</b> 2018/11/11 17:11.
 *
 * @author Hu Weihui
 */
public class PersistenceRDD {

    private static final String FILE_PATH
            = ActionRDD.class.getClassLoader().getResource("demo.txt").toString();

    private static final String OUTPUT_TXT_PATH
            = "D:/test/test/result";

    /**
     * 读文件
     *
     * @throws Exception
     */
    public void testReadFile() throws Exception {

        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);

        List<String> collect = stringJavaRDD.collect();
        checkResult(collect);
    }

    /**
     * 外部集合转成RDD
     */
    public void testParallelize() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> stringList = Arrays.asList("1", "2", "3", "4", "5");
        JavaRDD<String> parallelize = sparkContext.parallelize(stringList);
        List<String> collect = parallelize.collect();
        checkResult(collect);
    }


    private void checkResult(List<?> collect) {
        for (Object o : collect) {
            System.out.println(o.toString());
        }
    }
}
