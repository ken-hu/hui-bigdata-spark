package com.hui.bigdata.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * <b><code>SparkConfig</code></b>
 * <p/>
 * Description
 * <p/>
 * <b>Creation Time:</b> 2019/4/12 10:05.
 *
 * @author Hu-Weihui
 * @since hui-bigdata-springboot ${PROJECT_VERSION}
 */
@Component
@ConfigurationProperties(prefix = "spark")
@Data
public class SparkConfig {
    private String appName;

    private String master;

    private String sparkHome;
}
