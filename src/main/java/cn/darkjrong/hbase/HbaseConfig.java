package cn.darkjrong.hbase;

import cn.darkjrong.hbase.factory.RowKeyGeneratorFactory;
import cn.darkjrong.hbase.keygen.InputRowKeyGenerator;
import cn.darkjrong.hbase.keygen.SnowflakeIdRowKeyGenerator;
import cn.darkjrong.hbase.keygen.StringObjectIdRowKeyGenerator;
import cn.darkjrong.hbase.keygen.StringUUIDRowKeyGenerator;
import lombok.AllArgsConstructor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * hbase配置
 *
 * @author Rong.Jia
 * @date 2022/11/19
 */
@Configuration
@AllArgsConstructor
public class HbaseConfig {

    private final org.apache.hadoop.conf.Configuration configuration;

    @Bean
    public Connection getConnection() throws IOException {
        return ConnectionFactory.createConnection(configuration);
    }

    @Bean
    public HBaseAdmin hBaseAdmin() throws IOException {
        return (HBaseAdmin) getConnection().getAdmin();
    }

    @Bean
    public HbaseTemplate hbaseTemplate(Connection connection, HBaseAdmin admin) {
        return new HbaseTemplate(connection, admin);
    }

    @Bean
    public RowKeyGeneratorFactory keyGeneratorFactory() {
        return new RowKeyGeneratorFactory();
    }

    @Bean
    public SnowflakeIdRowKeyGenerator snowflakeIdKeyGenerator() {
        return new SnowflakeIdRowKeyGenerator();
    }

    @Bean
    public InputRowKeyGenerator inputKeyGenerator() {
        return new InputRowKeyGenerator();
    }

    @Bean
    public StringUUIDRowKeyGenerator stringUUIDKeyGenerator() {
        return new StringUUIDRowKeyGenerator();
    }

    @Bean
    public StringObjectIdRowKeyGenerator stringObjectIdRowKeyGenerator() {
        return new StringObjectIdRowKeyGenerator();
    }







}
