package cn.darkjrong.hbase;

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









}
