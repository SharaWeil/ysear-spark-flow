package com.ysear.spark.core.principal;

import com.ysear.spark.core.utils.LoginUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @author ysear
 * @date 2022/12/26
 */

public class KerborsHadoopPrincipal extends HadoopPrincipal {
    public final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";

    public final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    public final String zkServerPrincipal = "zookeeper/hadoop.hadoop.com";

    final String userPrincipal;

    final String userKeytabPath;

    final String krb5ConfPath;

    static final Logger LOG = LoggerFactory.getLogger(KerborsHadoopPrincipal.class);

    public KerborsHadoopPrincipal(String userPrincipal, String userKeytabPath, String krb5ConfPath) {
        this.userPrincipal = userPrincipal;
        this.userKeytabPath = userKeytabPath;
        this.krb5ConfPath = krb5ConfPath;
    }

    @Override
    public Configuration getPrincipalConf(Configuration conf, List<String> res) {
        for (String re : res) {
            if (!StringUtils.isBlank(re)) {
                LOG.info("add {} conf resource to hadoop config.", re);
                conf.addResource(re);
            }
        }
        try {
            LOG.info("try to setup hadoop kerberos auth.");
            LoginUtil.setKrb5Config(this.krb5ConfPath);
            LoginUtil.setJaasConf("Client", this.userPrincipal, this.userKeytabPath);
            LoginUtil.setZookeeperServerPrincipal("zookeeper.server.principal", "zookeeper/hadoop.hadoop.com");
            LoginUtil.login(this.userPrincipal, this.userKeytabPath, this.krb5ConfPath, conf);
            LOG.info("setup hadoop kerberos auth success.");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return conf;
    }
}

