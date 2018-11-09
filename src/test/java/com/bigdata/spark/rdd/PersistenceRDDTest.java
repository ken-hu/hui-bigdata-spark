package com.bigdata.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Serializable;

import java.util.Arrays;
import java.util.List;

/**
 * <b><code>PersistenceRDDTest</code></b>
 * <p/>
 * Description: DEMO-持久化RDD
 * <p/>
 * <b>Creation Time:</b> 2018/11/6 22:02.
 *
 * @author Hu Weihui
 */
public class PersistenceRDDTest implements Serializable {
    private static final String FILE_PATH = PersistenceRDDTest.class.getClassLoader().getResource("demo.txt").toString();


    private transient SparkConf sparkConf;
    private transient JavaSparkContext sparkContext;

    @Before
    public void before() throws Exception {
        sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        sparkContext = new JavaSparkContext(sparkConf);
    }

    @After
    public void after() throws Exception {
        sparkContext.close();
    }


    /**
     * 读文件
     *
     * @throws Exception
     */
    @Test
    public void testReadFile() throws Exception {

        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);

        List<String> collect = stringJavaRDD.collect();
        checkResult(collect);
    }

    /**
     * 外部集合转成RDD
     */
    @Test
    public void testParallelize() {
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
