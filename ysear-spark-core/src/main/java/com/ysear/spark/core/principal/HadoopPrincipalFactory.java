package com.ysear.spark.core.principal;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ysear
 * @date 2022/12/26
 */
public class HadoopPrincipalFactory {
    public static final String HADOOP_SECUROTY_AUTH = "hadoop.security.auth";

    public static final String HADOOP_SECUROTY_AUTH_NONE = "none";

    private static Logger LOG = LoggerFactory.getLogger(HadoopPrincipal.class);

    public static HadoopPrincipal getHadoopPrincipal(String userPrincipal, String userKeytabPath, String krb5ConfPath) {
        if (StringUtils.isBlank(userPrincipal)) {
            LOG.info("use HadoopPrincipal class: " + NonHadoopPrincipal.class.getName());
            return new NonHadoopPrincipal();
        }
        LOG.info("use HadoopPrincipal class: " + KerborsHadoopPrincipal.class.getName());
        return new KerborsHadoopPrincipal(userPrincipal, userKeytabPath, krb5ConfPath);
    }

    public static HadoopPrincipal getHadoopPrincipal(Configuration conf) {
        String auth = conf.get("hadoop.security.auth", "none");
        if (StringUtils.isBlank(auth) || "none".equals(auth)) {
            LOG.info("use HadoopPrincipal class: " + NonHadoopPrincipal.class.getName());
            return new NonHadoopPrincipal();
        }
        LOG.info("use HadoopPrincipal class: " + KerborsHadoopPrincipal.class.getName());
        String userPrincipal = conf.get("hadoop.security.auth.principal");
        String userKeytabPath = conf.get("hadoop.security.auth.keytab");
        String krb5ConfPath = conf.get("hadoop.security.auth.krb5Path");
        return new KerborsHadoopPrincipal(userPrincipal, userKeytabPath, krb5ConfPath);
    }
}

