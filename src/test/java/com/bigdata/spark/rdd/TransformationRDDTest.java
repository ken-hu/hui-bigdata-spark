package com.bigdata.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
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
    public void testMap(){
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        String title = new String("地铁站计算：");
        JavaRDD<String> resultMap = stringJavaRDD.map(x -> title+x);
        List<String> collect = resultMap.collect();
        checkResult(collect);
    }

    /**
     * Test flat map.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testFlatMap(){
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        JavaRDD<String> splitRDD = stringJavaRDD.flatMap(x -> Arrays.asList(x.split(",")).iterator());
        List<String> collect = splitRDD.collect();
        checkResult(collect);
    }

    /**
     * Test map to pair.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testMapToPair(){
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        List<Tuple2<String, String>> collect = stringJavaRDD.mapToPair(x -> new Tuple2<>(x, "测试一下谢谢")).collect();

        checkResult(collect);
    }

    /**
     * Test reduce by key.
     *
     * @since hui_project 1.0.0
     */
    @Test
    public void testReduceByKey(){
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        List<Tuple2<String, Integer>> collect = stringJavaRDD.map(x->Arrays.asList(x.split(",")).get(0))
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
    public void testGroupByKey(){
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        List<Tuple2<String, Iterable<Integer>>> collect = stringJavaRDD.map(x -> Arrays.asList(x.split(",")).get(0))
                .mapToPair(x -> new Tuple2<>(x, 1))
                .groupByKey().collect();
        checkResult(collect);
    }



    /**
     * 并集
     */
    @Test
    public void testUnionAndFilter(){
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        JavaRDD<String> result = stringJavaRDD.filter(x -> x.contains("广州南站"));
        JavaRDD<String> result1 = stringJavaRDD.filter(x -> x.contains("天河客运站"));
        JavaRDD<String> union = result.union(result1);
        List<String> collect = union.collect();
        System.out.println("-------"+union.count()+"-------");
        checkResult(collect);
    }

    @Test
    public void testMapPartitions(){
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
    }

    /**
     * Check result.
     *
     * @param collect the collect
     * @since hui_project 1.0.0
     */
    private void checkResult(List<?> collect){
        for (Object o : collect) {
            System.out.println(o.toString());
        }
    }
}
