package cn.darkjrong.hbase;

import cn.darkjrong.hbase.enums.IdType;
import cn.darkjrong.hbase.factory.RowKeyGeneratorFactory;
import cn.darkjrong.hbase.keygen.*;
import cn.darkjrong.spring.boot.autoconfigure.HbaseFactoryBean;
import cn.darkjrong.spring.boot.autoconfigure.HbaseProperties;
import cn.hutool.core.util.ReflectUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TestInit extends ObjectMappedFactoryTest {

    protected HbaseTemplate hbaseTemplate;
    protected RowKeyGeneratorFactory rowKeyGeneratorFactory;

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

        Map<IdType, RowKeyGenerator> idHandlerMap = new ConcurrentHashMap<>();
        idHandlerMap.put(IdType.ASSIGN_ID, new SnowflakeIdRowKeyGenerator());
        idHandlerMap.put(IdType.ASSIGN_UUID, new StringUUIDRowKeyGenerator());
        idHandlerMap.put(IdType.INPUT, new InputRowKeyGenerator());
        idHandlerMap.put(IdType.ASSIGN_OBJECT_ID, new StringObjectIdRowKeyGenerator());

        rowKeyGeneratorFactory = new RowKeyGeneratorFactory();
        ReflectUtil.setFieldValue(rowKeyGeneratorFactory, "idHandlerMap", idHandlerMap);
    }
















}
