package cn.darkjrong.spring.boot.autoconfigure;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

/**
 * hbase 自动配置
 *
 * @author Rong.Jia
 * @date 2022/11/19
 */
@ComponentScan("cn.darkjrong.hbase")
@AutoConfiguration
@EnableConfigurationProperties(HbaseProperties.class)
public class HbaseAutoConfiguration {

    @Bean
    public HbaseFactoryBean hbaseFactoryBean(HbaseProperties hbaseProperties) {
        return new HbaseFactoryBean(hbaseProperties);
    }















}
