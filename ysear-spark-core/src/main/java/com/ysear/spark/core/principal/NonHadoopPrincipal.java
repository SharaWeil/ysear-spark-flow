package com.ysear.spark.core.principal;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * @author ysear
 * @date 2022/12/26
 */
public class NonHadoopPrincipal extends HadoopPrincipal {
    @Override
    public Configuration getPrincipalConf(Configuration conf, List<String> res) {
        for (String re : res) {
            if (!StringUtils.isBlank(re)) {
                conf.addResource(re);
            }
        }
        return conf;
    }
}
