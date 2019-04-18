package com.hui.bigdata;

import com.hui.bigdata.common.spark.SparkJob;
import com.hui.bigdata.common.utils.SpringBootBeanUtils;
import com.hui.bigdata.config.SparkConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.Utils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * <b><code>SparkApplication</code></b>
 * <p/>
 * Description
 * <p/>
 * <b>Creation Time:</b> 2019/4/12 9:55.
 *
 * @author Hu-Weihui
 * @since hui-bigdata-springboot ${PROJECT_VERSION}
 */
@SpringBootApplication
public class SparkApplication implements CommandLineRunner {

    @Autowired
    private SparkConfig sparkConfig;

    public static void main(String[] args) {
        SpringApplication.run(SparkApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        // 初始化Spark环境
        SparkConf sparkConf = new SparkConf()
                .setAppName(sparkConfig.getAppName())
                .setMaster(sparkConfig.getMaster());

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        String className = args[0];
        Class clazz = Utils.classForName(className);
        Object sparkJob = SpringBootBeanUtils.getBean(clazz);
        if (sparkJob instanceof SparkJob){
            ((SparkJob) sparkJob).execute(javaSparkContext);
        }
    }
}
