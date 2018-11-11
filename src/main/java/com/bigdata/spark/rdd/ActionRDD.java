package com.bigdata.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.beans.Transient;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * <b><code>ActionRDD</code></b>
 * <p/>
 * Description:
 * <p/>
 * <b>Creation Time:</b> 2018/11/11 17:09.
 *
 * @author Hu Weihui
 */
public class ActionRDD {

    private static final String FILE_PATH
            = ActionRDD.class.getClassLoader().getResource("demo.txt").toString();

    private static final String OUTPUT_TXT_PATH
            = "D:/test/test/result";


    /**
     * 聚合（整合数据）.
     *
     * @since hui_project 1.0.0
     */
    public void testReduce() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
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
    public void testCollect() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        List<String> collect = stringJavaRDD.collect();
        checkResult(collect);
    }

    /**
     * 集合里面元素数量.
     *
     * @since hui_project 1.0.0
     */
    public void testCount() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        long count = stringJavaRDD.count();
        System.out.println(count);
    }

    /**
     * 取第一个元素.
     *
     * @since hui_project 1.0.0
     */
    public void testFirst() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        String first = stringJavaRDD.first();
        System.out.println(first);
    }

    /**
     * 取前N个数.
     *
     * @since hui_project 1.0.0
     */
    public void testTake() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        List<String> take = stringJavaRDD.take(10);
        System.out.println(take);

    }

    /**
     * 取Key对应元素数量
     *
     * @since hui_project 1.0.0
     */
    public void testCountByKey(){
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
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
    public void testForEach(){
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
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
