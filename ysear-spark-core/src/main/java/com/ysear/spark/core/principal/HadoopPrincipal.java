package com.ysear.spark.core.principal;


import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * @author ysear
 * @date 2022/12/26
 */

public abstract class HadoopPrincipal {
    public Configuration getConf(List<String> res) {
        return getPrincipalConf(new Configuration(), res);
    }

    public abstract Configuration getPrincipalConf(Configuration paramConfiguration, List<String> paramVarArgs);
}
