package ssy.kafka.common;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * author: huangqian
 * date  : 15/9/10
 * time  : 下午4:06
 */
public class PropertiesUtils {

    private static final Logger LOG = Logger.getLogger(PropertiesUtils.class);

    public static Properties loadFile(String propFile) {
        Toolkit.ifNull(propFile,"propFile文件不能为空");
        InputStream inputStream = PropertiesUtils.class.getResourceAsStream(propFile);
        Properties prop = new Properties();
        if(Toolkit.isNotNull(inputStream)) {
            try {
                prop.load(inputStream);
            } catch (IOException e) {
                LOG.error("load config  from file error",e);
            }
        }
        return prop;
    }
}
