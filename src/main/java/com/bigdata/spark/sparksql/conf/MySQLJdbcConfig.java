package com.bigdata.spark.sparksql.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * <b><code>MySQLJdbcConfig</code></b>
 * <p/>
 * Description:
 * <p/>
 * <b>Creation Time:</b> 2018/11/11 17:32.
 *
 * @author Hu Weihui
 */
public class MySQLJdbcConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLJdbcConfig.class);

    private String table;

    private String url;

    private Properties connectionProperties;

    public void init(){
        Properties properties = new Properties();
        InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream("jdbc.properties");
        try {
            properties.load(resourceAsStream);
            setUrl(properties.getProperty("db.url"));
            //考虑多数据源的情况，另外创建properties传入
            Properties connectionProperties = new Properties();
            connectionProperties.setProperty("user",properties.getProperty("db.user"));
            connectionProperties.setProperty("password",properties.getProperty("db.password"));
            connectionProperties.setProperty("url",properties.getProperty("db.url"));
            setConnectionProperties(connectionProperties);
        } catch (IOException e) {
            LOGGER.info("读取配置文件失败");
        }

    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Properties getConnectionProperties() {
        return connectionProperties;
    }

    public void setConnectionProperties(Properties connectionProperties) {

        this.connectionProperties = connectionProperties;
    }
}
