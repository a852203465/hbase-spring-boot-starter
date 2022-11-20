package cn.darkjrong.spring.boot.autoconfigure;

import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * hbase工厂bean
 *
 * @author Rong.Jia
 * @date 2022/11/19
 */
@Slf4j
public class HbaseFactoryBean implements FactoryBean<Configuration>, InitializingBean {

    private Configuration configuration;
    private final HbaseProperties hbaseProperties;

    public HbaseFactoryBean(HbaseProperties hbaseProperties) {
        this.hbaseProperties = hbaseProperties;
    }

    @Override
    public void afterPropertiesSet() {

        Assert.notBlank(hbaseProperties.getQuorum(), "The given 'quorum' must not be null!");
        Assert.notBlank(hbaseProperties.getRootDir(), "The given 'rootDir' must not be null!");
        Assert.notBlank(hbaseProperties.getNodeParent(), "The given 'nodeParent' must not be null!");
        Assert.isTrue(StrUtil.startWith(hbaseProperties.getNodeParent(), StrUtil.SLASH), "Path must start with / character");

        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, this.hbaseProperties.getQuorum());
        configuration.set(HConstants.HBASE_DIR, this.hbaseProperties.getRootDir());
        configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, this.hbaseProperties.getNodeParent());
        this.configuration = configuration;
    }

    @Override
    public Configuration getObject() {
        return this.configuration;
    }

    @Override
    public Class<?> getObjectType() {
        return Configuration.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
