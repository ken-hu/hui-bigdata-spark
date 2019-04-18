package com.bigdata.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.beans.Transient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * <b><code>TransformationRDD</code></b>
 * <p/>
 * Description:
 * <p/>
 * <b>Creation Time:</b> 2018/11/11 17:14.
 *
 * @author Hu Weihui
 */
public class TransformationRDD {

    private static final String FILE_PATH
            = ActionRDD.class.getClassLoader().getResource("demo.txt").toString();

    private static final String OUTPUT_TXT_PATH
            = "D:/test/test/result";

    /**
     * 元素转换 . 参数->你希望要的参数
     * demo计算目的：获取地铁站名字
     *
     * @since hui_project 1.0.0
     */
    public void testMap() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        JavaRDD<String> resultMap = textRDD.map(x -> x.split(",")[0]);
        checkResult(resultMap.collect());
    }

    /**
     * 元素转换. 参数->数组参数
     * demo计算目的：获取地铁站信息切分后 获取数组信息1.出发站 2.终点站 3.经历站点数 4.距离
     *
     * @since hui_project 1.0.0
     */
    public void testFlatMap() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        JavaRDD<String> splitRDD = textRDD
                .flatMap(x -> Arrays.asList(x.split(",")).iterator());
        checkResult(splitRDD.collect());
    }

    /**
     * 元素转换. 参数->pair参数 (key value).
     * demo计算目的 : 没想到有什么东西,随便写写 只是把 地铁名字 -> key:地铁名字 value:1
     *
     * @since hui_project 1.0.0
     */
    public void testMapToPair() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        List<Tuple2<String, Integer>> collect = textRDD
                .map(x -> x.split(",")[0])
                .mapToPair(x -> new Tuple2<>(x, 1)).collect();
        checkResult(collect);
    }

    /**
     * 聚合.
     * demo计算目的: 计算每个地铁站名字出现次数
     *
     * @since hui_project 1.0.0
     */
    public void testReduceByKey() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        JavaPairRDD<String, Integer> rdd = textRDD
                .map(x -> Arrays.asList(x.split(",")).get(0))
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> x + y);
        checkResult(rdd.collect());

    }

    /**
     * 分组. groupByKey通过 key值分组，value是数组输出
     * demo逻辑计算：进站->进站[出站A,出站B，....，出站N]
     *
     * @since hui_project 1.0.0
     */
    public void testGroupByKey() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        JavaPairRDD<String, Iterable<String>> rdd = textRDD
                .mapToPair(x -> new Tuple2<>(x.split(",")[0], x.split(",")[1]))
                .groupByKey();
        checkResult(rdd.collect());
    }

    /**
     * 集合并集.
     * demo计算目的：找出所有进站是广南和天河客运站的信息
     *
     * @since hui_project 1.0.0
     */
    public void testUnionAndFilter() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        JavaRDD<String> result = textRDD.filter(x -> x.contains("广州南站"));
        JavaRDD<String> result1 = textRDD.filter(x -> x.contains("天河客运站"));
        JavaRDD<String> union = result.union(result1);
        System.out.println("-------" + union.count() + "-------");
        checkResult(union.collect());
    }

    /**
     * 元素转换,在每一个分区内部进行元素转换.
     * demo计算目的：算平方。（单元测试比较难看出来分区作用）
     *
     * @since hui_project 1.0.0
     */
    public void testMapPartitions() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> parallelize = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);
        JavaRDD<Tuple2<Integer, Integer>> rdd = parallelize
                .mapPartitions(x -> getSquare(x));
        checkResult(rdd.collect());

    }

    /**
     * Gets square.
     *
     * @param it the it
     * @return the square
     * @since hui_project 1.0.0
     */
    @Transient
    private Iterator<Tuple2<Integer, Integer>> getSquare(Iterator<Integer> it) {
        ArrayList<Tuple2<Integer, Integer>> results = new ArrayList<>();
        while (it.hasNext()) {
            Integer next = it.next();
            results.add(new Tuple2<>(next, next * next));
        }
        return results.iterator();
    }

    /**
     * 元素转换,在每一个分区内部进行元素转换.
     * demo计算目的：算平方。（参数1是分区的索引）
     *
     * @since hui_project 1.0.0
     */
    public void testMapPartitionsWithIndex() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> parallelize = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);
        JavaRDD<Tuple2<Integer, Integer>> rdd = parallelize.mapPartitionsWithIndex((x, y) -> getSquareWithIndex(x, y), false);
        checkResult(rdd.collect());
    }

    /**
     * Get square with index iterator.
     *
     * @param partIndex the part index
     * @param it        the it
     * @return the iterator
     * @since hui_project 1.0.0
     */
    @Transient
    public Iterator<Tuple2<Integer, Integer>> getSquareWithIndex(Integer partIndex, Iterator<Integer> it) {
        ArrayList<Tuple2<Integer, Integer>> results = new ArrayList<>();
        while (it.hasNext()) {
            Integer next = it.next();
            results.add(new Tuple2<>(partIndex, next * next));
        }
        return results.iterator();
    }


    /**
     * 元素采样.
     * true 元素可以多次采样
     *
     * @since hui_project 1.0.0
     */
    public void testSample() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        //元素可以多次采样
        JavaRDD<String> sample = textRDD
                .sample(true, 0.001, 100);
        checkResult(sample.collect());
    }

    /**
     * 元素采样.
     * false 元素不可以多次采样
     *
     * @since hui_project 1.0.0
     */
    public void testSample2() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        JavaRDD<String> sample = textRDD.sample(false, 0.001, 100);
        checkResult(sample.collect());
    }

    /**
     * 集合去重.
     *
     * @since hui_project 1.0.0
     */
    public void testDistinct() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        System.out.println("source text count :" + textRDD.count());
        JavaRDD<String> distinct = textRDD.map(x -> x.split(",")[0])
                .distinct();
        System.out.println("distinct count :" + distinct.count());
        checkResult(distinct.collect());
    }

    /**
     * 排序
     * false为倒序 true顺序
     *
     * @since hui_project 1.0.0
     */
    public void testSortByKey() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> textRDD = sparkContext.textFile(FILE_PATH);
        JavaPairRDD<Integer, String> rdd = textRDD.map(x -> Arrays.asList(x.split(",")).get(0))
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(x -> new Tuple2<>(x._2, x._1))
                .sortByKey(false);//倒序
        checkResult(rdd.collect());

    }

    /**
     * 集合关联. 合并相同key的value
     * demo计算目的：今年和去年都获奖的同学，获奖项的科目都有哪些
     *
     * @since hui_project 1.0.0
     */
    public void testJoin() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //今年同学获奖的科目
        JavaPairRDD<Object, Object> rdd1 = sparkContext.parallelize(Arrays.asList(
                new Tuple2("xiaoming", "语文")
                , new Tuple2("xiaoming", "数学")
                , new Tuple2("lihua", "数学")
                , new Tuple2("xiaofeng", "艺术")
                , new Tuple2("test", "艺术")))
                .mapToPair(x -> new Tuple2<>(x._1, x._2));
        //去年同学获奖的科目
        JavaPairRDD<Object, Object> rdd2 = sparkContext.parallelize(Arrays.asList(
                new Tuple2("xiaoming", "艺术")
                , new Tuple2("lihua", "艺术")
                , new Tuple2("xiaofeng", "语文")))
                .mapToPair(x -> new Tuple2<>(x._1, x._2));
        JavaPairRDD<Object, Tuple2<Object, Object>> join = rdd1.join(rdd2);
        checkResult(join.collect());
    }

    /**
     * Test co group.
     * demo计算目的: 以成绩分组 同学([成绩优秀学科],[成绩中等学科],[成绩差劲学科])
     *
     * @since hui_project 1.0.0
     */
    public void testCoGroup() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //成绩优秀的学生+科目
        JavaRDD<Tuple2<String, String>> scoreDetails1 = sparkContext.parallelize(Arrays.asList(
                new Tuple2("xiaoming", "语文")
                , new Tuple2("xiaoming", "数学")
                , new Tuple2("lihua", "数学")
                , new Tuple2("xiaofeng", "艺术")));
        //成绩中等的学生+科目
        JavaRDD<Tuple2<String, String>> scoreDetails2 = sparkContext.parallelize(Arrays.asList(
                new Tuple2("xiaoming", "艺术")
                , new Tuple2("lihua", "艺术")
                , new Tuple2("xiaofeng", "语文")));
        //成绩差的学生+科目
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
     * demo计算目的：超人VS怪兽所有组合
     *
     * @since hui_project 1.0.0
     */
    public void testCartesain() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
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
