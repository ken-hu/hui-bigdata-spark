package com.bigdata.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * <b><code>TransformationRDDTest</code></b>
 * <p/>
 * Description: DEMO - TransFormation RDD
 * <p/>
 * <b>Creation Time:</b> 2018/11/6 0:22.
 *
 * @author Hu Weihui
 */
public class TransformationRDDTest {

    /**
     * The constant FILE_PATH.
     *
     * @since hui_project 1.0.0
     */
    private static final String FILE_PATH = TransformationRDDTest.class.getClassLoader().getResource("demo.txt").toString();

    /**
     * The Spark conf.
     *
     * @since hui_project 1.0.0
     */
    private SparkConf sparkConf;
    /**
     * The Spark context.
     *
     * @since hui_project 1.0.0
     */
    private JavaSparkContext sparkContext;

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


    /**
     * Test map.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testMap() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        String title = new String("地铁站计算：");
        JavaRDD<String> resultMap = textRDD.map(x -> title + x);
        List<String> collect = resultMap.collect();
        checkResult(collect);
    }

    /**
     * Test flat map.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testFlatMap() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        JavaRDD<String> splitRDD = textRDD.flatMap(x -> Arrays.asList(x.split(",")).iterator());
        List<String> collect = splitRDD.collect();
        checkResult(collect);
    }

    /**
     * Test map to pair.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testMapToPair() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        List<Tuple2<String, String>> collect = textRDD.mapToPair(x -> new Tuple2<>(x, "测试一下谢谢")).collect();

        checkResult(collect);
    }

    /**
     * Test reduce by key.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testReduceByKey() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        List<Tuple2<String, Integer>> collect = textRDD.map(x -> Arrays.asList(x.split(",")).get(0))
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> x + y).collect();
        checkResult(collect);

    }

    /**
     * 测试groupByKey.
     * groupByKey通过 key值分组，value是数组输出
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testGroupByKey() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        List<Tuple2<String, Iterable<Integer>>> collect = textRDD.map(x -> Arrays.asList(x.split(",")).get(0))
                .mapToPair(x -> new Tuple2<>(x, 1))
                .groupByKey().collect();
        checkResult(collect);
    }


    /**
     * 并集
     */
    @Test
    public void testUnionAndFilter() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        JavaRDD<String> result = textRDD.filter(x -> x.contains("广州南站"));
        JavaRDD<String> result1 = textRDD.filter(x -> x.contains("天河客运站"));
        JavaRDD<String> union = result.union(result1);
        List<String> collect = union.collect();
        System.out.println("-------" + union.count() + "-------");
        checkResult(collect);
    }

    @Test
    public void testMapPartitions() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
    }

    @Test
    public void testSample() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        //元素可以多次采样
        List<String> collect = textRDD.sample(true, 0.001, 100).collect();
        checkResult(collect);
    }

    @Test
    public void testSample2() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        List<String> collect = textRDD.sample(false, 0.001, 100).collect();
        checkResult(collect);
    }

    @Test
    public void testDistinct() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        JavaRDD<String> distinct = textRDD.map(x -> Arrays.asList(x.split(",")).get(0))
                .distinct();
        checkResult(distinct.collect());
    }

    @Test
    public void testSortByKey() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        JavaPairRDD<Integer, String> rdd = textRDD.map(x -> Arrays.asList(x.split(",")).get(0))
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(x -> new Tuple2<>(x._2, x._1))
                .sortByKey(false);//倒序
        checkResult(rdd.collect());

    }

    @Test
    public void testJoin() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        JavaPairRDD<String, String> pairRDD1 = textRDD.mapToPair(x -> new Tuple2<>(x, "rdd1"));
        JavaPairRDD<String, String> pairRDD2 = textRDD.mapToPair(x -> new Tuple2<>(x, "rdd2"));
        JavaPairRDD<String, Tuple2<String, String>> join = pairRDD1.join(pairRDD2);
        checkResult(join.collect());
    }

    @Test
    public void testCoGroup() {
        JavaRDD<Tuple2<String,Float>> scoreDetails1 = sparkContext.parallelize(Arrays.asList(
                new Tuple2("xiaoming", "语文")
                , new Tuple2("xiaoming", "数学")
                , new Tuple2("lihua", "数学")
                , new Tuple2("xiaofeng", "艺术")));
        JavaRDD<Tuple2<String,Float>> scoreDetails2 = sparkContext.parallelize(Arrays.asList(
                new Tuple2("xiaoming", "艺术")
                , new Tuple2("lihua", "艺术")
                , new Tuple2("xiaofeng", "语文")));
        JavaRDD<Tuple2<String,Float>> scoreDetails3 = sparkContext.parallelize(Arrays.asList(
                new Tuple2("xiaoming", "英语")
                , new Tuple2("lihua", "英语")
                , new Tuple2("lihua", "数学")
                , new Tuple2("xiaofeng", "数学")
                , new Tuple2("xiaofeng", "英语")));

        JavaPairRDD<String, Float> scoreMapRDD1 = JavaPairRDD.fromJavaRDD(scoreDetails1);
        JavaPairRDD<String, Float> scoreMapRDD2 = JavaPairRDD.fromJavaRDD(scoreDetails2);
        JavaPairRDD<String, Float> scoreMapRDD3 = JavaPairRDD.fromJavaRDD(scoreDetails3);

        JavaPairRDD<String, Tuple3<Iterable<Float>, Iterable<Float>, Iterable<Float>>> cogroupRDD =
                scoreMapRDD1.cogroup(scoreMapRDD2, scoreMapRDD3);
        checkResult(cogroupRDD.collect());
    }

    /**
     * Check result.
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
