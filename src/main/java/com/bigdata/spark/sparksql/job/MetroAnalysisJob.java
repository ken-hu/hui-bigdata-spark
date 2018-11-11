package com.bigdata.spark.sparksql.job;

import com.bigdata.spark.common.SparkJob;
import com.bigdata.spark.common.util.SparkJobUtil;
import com.bigdata.spark.sparksql.conf.MySQLJdbcConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <b><code>MetroAnalysisJob</code></b>
 * <p/>
 * Description:
 * <p/>
 * <b>Creation Time:</b> 2018/11/11 17:32.
 *
 * @author Hu Weihui
 */
public class MetroAnalysisJob extends SparkJob {

    private static Logger LOGGER = LoggerFactory.getLogger(MetroAnalysisJob.class);

    private static final String INPUT_FILE_PATH
            = MetroAnalysisJob.class.getClassLoader().getResource("test.json").toString();

    private static final String OUTPUT_FILE_PATH
            = "D:/test/test";

    private static final String SQL = "select * from hui_metro_testjson";

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("test")
                .setMaster("local[4]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        MetroAnalysisJob metroAnalysisJob = new MetroAnalysisJob();

        metroAnalysisJob.execute(sparkContext, args);
    }

    @Override
    public void execute(JavaSparkContext sparkContext, String[] args) {
        super.execute(sparkContext, args);
        deal(sparkContext, INPUT_FILE_PATH, OUTPUT_FILE_PATH);
    }

    /**
     * 数据逻辑处理
     * @param sparkContext
     * @param inPutPath
     * @param outPutPath
     */
    private void deal(JavaSparkContext sparkContext, String inPutPath, String outPutPath) {
        SparkJobUtil.checkFileExists(inPutPath);

        SQLContext sqlContext = new SQLContext(sparkContext);
//        sqlContext.setConf("spark.sql.parquet.binaryAsString","true");

        //创建快照临时表
        Dataset<Row> dataset = sqlContext.read().json(inPutPath);
        dataset.registerTempTable("hui_metro_testjson");
        dataset.show(10);

        Dataset<Row> resultFrame = sqlContext.sql(SQL);

        if (resultFrame.count() > 0) {
            resultFrame.repartition(3).write()
                    .mode(SaveMode.Append).json(outPutPath);
        }

        resultFrame.show(10);

        //结果写入数据库
        MySQLJdbcConfig jdbcConfig = new MySQLJdbcConfig();
        jdbcConfig.init();
        resultFrame.write().mode("append")
                .jdbc(jdbcConfig.getUrl(), "hui_metro_test", jdbcConfig.getConnectionProperties());
    }

}
