package com.bigdata.spark.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * <b><code>SparkJobUtil</code></b>
 * <p/>
 * Description:
 * <p/>
 * <b>Creation Time:</b> 2018/11/11 17:48.
 *
 * @author Hu Weihui
 */
public class SparkJobUtil {
    /**
     * The constant LOGGER.
     *
     * @since hui_project 1.0.0
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkJobUtil.class);

    /**
     * Close quietly.
     *
     * @param fileSystem the file system
     * @since hui_project 1.0.0
     */
    public static void closeQuietly(FileSystem fileSystem) {
        if (fileSystem != null) {
            try {
                fileSystem.close();
            } catch (IOException e) {
                LOGGER.error("Fail to close FileSystem:" + fileSystem, e);
            }
        }
    }

    /**
     * Check file exists.
     *
     * @param path the path
     * @since hui_project 1.0.0
     */
    public static void checkFileExists(String path) {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(configuration);
            if (!fileSystem.exists(new Path(path))) {
                throw new FileNotFoundException(path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            closeQuietly(fileSystem);
        }
    }
}
