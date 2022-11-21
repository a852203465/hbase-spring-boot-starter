package cn.darkjrong.hbase;

import cn.darkjrong.hbase.repository.ObjectParser;
import cn.darkjrong.spring.boot.autoconfigure.HbaseFactoryBean;
import cn.darkjrong.spring.boot.autoconfigure.HbaseProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

public class TestInit extends ObjectMappedFactoryTest {

    protected HbaseTemplate hbaseTemplate;
    protected ObjectParser objectParser;

    @BeforeEach
   public void testInit() throws IOException {
        HbaseProperties hbaseProperties = new HbaseProperties();
        hbaseProperties.setQuorum("127.0.0.1:2181");
        hbaseProperties.setRootDir("hdfs://localhost:8020/hbase");
        hbaseProperties.setNodeParent("/hbase");
        hbaseProperties.setTableSanityChecks(Boolean.TRUE);
        HbaseFactoryBean hbaseFactoryBean = new HbaseFactoryBean(hbaseProperties);
        hbaseFactoryBean.afterPropertiesSet();
        Configuration configuration = hbaseFactoryBean.getObject();
        Connection connection = ConnectionFactory.createConnection(configuration);
        HBaseAdmin hBaseAdmin = (HBaseAdmin) connection.getAdmin();
        hbaseTemplate = new HbaseTemplate(connection, hBaseAdmin);
        objectParser = new ObjectParser(objectMappedFactory);
    }


}
