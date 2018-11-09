package com.bigdata.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Serializable;
import scala.Tuple2;

import java.beans.Transient;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * <b><code>ActionRDDTest</code></b>
 * <p/>
 * Description:DEMO - Action RDD
 * <p/>
 * <b>Creation Time:</b> 2018/11/6 0:27.
 *
 * @author Hu Weihui
 */
public class ActionRDDTest implements Serializable {

    /**
     * The constant FILE_PATH.
     *
     * @since hui_project 1.0.0
     */
    private static final String FILE_PATH
            = ActionRDDTest.class.getClassLoader().getResource("demo.txt").toString();

    private static final String OUTPUT_TXT_PATH
            = "D:/test/test/result";


    /**
     * The Spark conf.
     *
     * @since hui_project 1.0.0
     */
    private transient SparkConf sparkConf;
    /**
     * The Spark context.
     *
     * @since hui_project 1.0.0
     */
    private transient JavaSparkContext sparkContext;

    /**
     * Before.
     *
     * @throws Exception the exception
     * @since hui_project 1.0.0
     */
    @Before
    public void before() throws Exception {
        sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        sparkContext = new JavaSparkContext(sparkConf);
    }

    /**
     * After.
     *
     * @throws Exception the exception
     * @since hui_project 1.0.0
     */
    @After
    public void after() throws Exception {
        sparkContext.close();
    }

    @Test
    public void saveAsTxtFile() throws Exception{
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        stringJavaRDD.saveAsTextFile(OUTPUT_TXT_PATH);
    }

    /**
     * 聚合（整合数据）.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testReduce() {
        JavaRDD<Integer> parallelize = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4), 3);
        Tuple2<Integer, Integer> reduce = parallelize.mapToPair(x -> new Tuple2<>(x, 1))
                .reduce((x, y) -> getReduce(x, y));
        System.out.println("数组sum:" + reduce._1 + " 计算次数:" + (reduce._2 - 1));
    }

    /**
     * 计算逻辑.
     * （x）总和->数组的每一个数相加总和
     * （y）总和 ->计算次数
     * @param x the x
     * @param y the y
     * @return the reduce
     * @since hui_project 1.0.0
     */
    @Transient
    public Tuple2 getReduce(Tuple2<Integer, Integer> x, Tuple2<Integer, Integer> y) {
        Integer a = x._1();
        Integer b = x._2();
        a += y._1();
        b += y._2();
        return new Tuple2(a, b);
    }

    /**
     * 收集所有元素.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testCollect() {
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        List<String> collect = stringJavaRDD.collect();
        checkResult(collect);
    }

    /**
     * 集合里面元素数量.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testCount() {
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        long count = stringJavaRDD.count();
        System.out.println(count);
    }

    /**
     * 取第一个元素.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testFirst() {
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        String first = stringJavaRDD.first();
        System.out.println(first);
    }

    /**
     * 取前N个数.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testTake() {
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        List<String> take = stringJavaRDD.take(10);
        System.out.println(take);

    }

    /**
     * 取Key对应元素数量
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testCountByKey(){
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        Map<String, Long> stringLongMap = stringJavaRDD.map(x -> x.split(",")[0])
                .mapToPair(x -> new Tuple2<>(x, 1))
                .countByKey();

        for (String key : stringLongMap.keySet()) {
            System.out.println(key + " + " + stringLongMap.get(key) );
        }
    }

    /**
     * 循环
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testForEach(){
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        stringJavaRDD.foreach(x->{
            System.out.println(x);
        });
    }
    /**
     * 打印测试.
     *
     * @param collect the collect
     * @since hui_project 1.0.0
     */
    private void checkResult(List<?> collect) {
        for (Object o : collect) {
            System.out.println(o.toString());
        }
    }

}
