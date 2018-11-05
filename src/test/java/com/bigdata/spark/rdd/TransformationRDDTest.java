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
 * Description:
 * <p/>
 * <b>Creation Time:</b> 2018/11/6 0:22.
 *
 * @author Hu Weihui
 */
public class TransformationRDDTest {

    private static final String FILE_PATH = TransformationRDDTest.class.getClassLoader().getResource("demo.txt").toString();

    private SparkConf sparkConf;
    private JavaSparkContext sparkContext;
    @Before
    public void before() throws Exception {
        sparkConf = new SparkConf().setMaster("local[4]").setAppName("test");
        sparkContext = new JavaSparkContext(sparkConf);
    }

    @After
    public void after() throws Exception {
        sparkContext.close();
    }


    @Test
    public void testMap(){
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        String title = new String("地铁站计算：");
        JavaRDD<String> resultMap = stringJavaRDD.map(x -> title+x);
        List<String> collect = resultMap.collect();
        checkResult(collect);
    }

    @Test
    public void testFlatMap(){
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        JavaRDD<String> splitRDD = stringJavaRDD.flatMap(x -> Arrays.asList(x.split(",")).iterator());
        List<String> collect = splitRDD.collect();
        checkResult(collect);
    }

    @Test
    public void testMapToPair(){
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILE_PATH);
        List<Tuple2<String, String>> collect = stringJavaRDD.mapToPair(x -> new Tuple2<>(x, "测试一下谢谢")).collect();
        checkResult(collect);
    }

    /**
     * 读文件
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
    public void testParallelize(){
        List<String> stringList = Arrays.asList("1", "2", "3", "4", "5");
        JavaRDD<String> parallelize = sparkContext.parallelize(stringList);
        List<String> collect = parallelize.collect();
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



    private void checkResult(List<?> collect){
        for (Object o : collect) {
            System.out.println(o.toString());
        }
    }
}
