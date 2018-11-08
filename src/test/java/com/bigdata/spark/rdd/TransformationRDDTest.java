package com.bigdata.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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
    private static final String FILE_PATH = TransformationRDDTest.class.getClassLoader().getResource("demo.txt")
            .toString();

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
     * 初始化
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
     * 元素转换.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testMap() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        JavaRDD<String> resultMap = textRDD.map(x -> x.split(",")[0]);
        checkResult(resultMap.collect());
    }

    /**
     * Test flat map.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testFlatMap() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        JavaRDD<String> splitRDD = textRDD
                .flatMap(x -> Arrays.asList(x.split(",")).iterator());
        checkResult(splitRDD.collect());
    }

    /**
     * Test map to pair.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testMapToPair() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        List<Tuple2<String, Integer>> collect = textRDD
                .map(x -> x.split(",")[0])
                .mapToPair(x -> new Tuple2<>(x, 1)).collect();
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
        JavaPairRDD<String, Integer> rdd = textRDD
                .map(x -> Arrays.asList(x.split(",")).get(0))
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> x + y);
        checkResult(rdd.collect());

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
        JavaPairRDD<String, Iterable<String>> rdd = textRDD
                .mapToPair(x -> new Tuple2<>(x.split(",")[0], x.split(",")[1]))
                .groupByKey();
        checkResult(rdd.collect());
    }

    /**
     * Test union and filter.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testUnionAndFilter() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        JavaRDD<String> result = textRDD.filter(x -> x.contains("广州南站"));
        JavaRDD<String> result1 = textRDD.filter(x -> x.contains("天河客运站"));
        JavaRDD<String> union = result.union(result1);
        System.out.println("-------" + union.count() + "-------");
        checkResult(union.collect());
    }

    /**
     * Test map partitions.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testMapPartitions() {
        JavaRDD<Integer> parallelize = sparkContext.parallelize(Arrays.asList(1, 2, 3));
        JavaRDD<Integer> rdd = parallelize.mapPartitions(x -> getSquare(x));
        checkResult(rdd.collect());

    }

    private Iterator<Integer> getSquare(Iterator<Integer> it){
        ArrayList<Integer> results = new ArrayList<>();
        while(it.hasNext()){
            Integer next = it.next();
            results.add(next * next);
        }
        return results.iterator();
    }

    /**
     * Test map partitions with split.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testMapPartitionsWithSplit() {

    }


    /**
     * Test sample.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testSample() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        //元素可以多次采样
        JavaRDD<String> sample = textRDD
                .sample(true, 0.001, 100);
        checkResult(sample.collect());
    }

    /**
     * Test sample 2.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testSample2() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        JavaRDD<String> sample = textRDD.sample(false, 0.001, 100);
        checkResult(sample.collect());
    }

    /**
     * 去重.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testDistinct() {
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        System.out.println("source text count :" + textRDD.count());
        JavaRDD<String> distinct = textRDD.map(x -> x.split(",")[0])
                .distinct();
        System.out.println("distinct count :" + distinct.count());
        checkResult(distinct.collect());
    }

    /**
     * false为倒序 true顺序
     *
     * @since hui_project 1.0.0
     */
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

    /**
     * Test join.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testJoin() {
        JavaPairRDD<Object, Object> rdd1 = sparkContext.parallelize(Arrays.asList(
                new Tuple2("xiaoming", "语文")
                , new Tuple2("xiaoming", "数学")
                , new Tuple2("lihua", "数学")
                , new Tuple2("xiaofeng", "艺术")))
                .mapToPair(x -> new Tuple2<>(x._1, x._2));
        JavaPairRDD<Object, Object> rdd2 = sparkContext.parallelize(Arrays.asList(
                new Tuple2("xiaoming", "艺术")
                , new Tuple2("lihua", "艺术")
                , new Tuple2("xiaofeng", "语文")
                , new Tuple2("test", "艺术")))
                .mapToPair(x -> new Tuple2<>(x._1, x._2));
        JavaPairRDD<Object, Tuple2<Object, Object>> join = rdd1.join(rdd2);
        checkResult(join.collect());
    }

    /**
     * Test co group.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testCoGroup() {
        JavaRDD<Tuple2<String, String>> scoreDetails1 = sparkContext.parallelize(Arrays.asList(
                new Tuple2("xiaoming", "语文")
                , new Tuple2("xiaoming", "数学")
                , new Tuple2("lihua", "数学")
                , new Tuple2("xiaofeng", "艺术")));
        JavaRDD<Tuple2<String, String>> scoreDetails2 = sparkContext.parallelize(Arrays.asList(
                new Tuple2("xiaoming", "艺术")
                , new Tuple2("lihua", "艺术")
                , new Tuple2("xiaofeng", "语文")));
        JavaRDD<Tuple2<String, String>> scoreDetails3 = sparkContext.parallelize(Arrays.asList(
                new Tuple2("xiaoming", "英语")
                , new Tuple2("lihua", "英语")
                , new Tuple2("lihua", "数学")
                , new Tuple2("xiaofeng", "数学")
                , new Tuple2("xiaofeng", "英语")));

        JavaPairRDD<String, String> scoreMapRDD1 = JavaPairRDD.fromJavaRDD(scoreDetails1);
        JavaPairRDD<String, String> scoreMapRDD2 = JavaPairRDD.fromJavaRDD(scoreDetails2);
        JavaPairRDD<String, String> scoreMapRDD3 = JavaPairRDD.fromJavaRDD(scoreDetails3);

        JavaPairRDD<String, Tuple3<Iterable<String>, Iterable<String>, Iterable<String>>> cogroupRDD =
                scoreMapRDD1.cogroup(scoreMapRDD2, scoreMapRDD3);
        checkResult(cogroupRDD.collect());
    }

    /**
     * 笛卡尔积.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testCartesain() {
        JavaRDD<String> list1 = sparkContext.parallelize(Arrays.asList("咸蛋超人VS", "蒙面超人VS", "奥特曼VS"));
        JavaRDD<String> list2 = sparkContext.parallelize(Arrays.asList("小怪兽", "中怪兽", "大怪兽"));
        JavaPairRDD<String, String> cartesian = list1.cartesian(list2);
        checkResult(cartesian.collect());
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
