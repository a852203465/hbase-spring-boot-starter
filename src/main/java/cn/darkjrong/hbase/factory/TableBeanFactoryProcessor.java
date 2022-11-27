package cn.darkjrong.hbase.factory;

import cn.darkjrong.hbase.HbaseTemplate;
import cn.darkjrong.hbase.domain.ObjectMappedStatement;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.ReflectUtil;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 表bean工厂处理器
 *
 * @author Rong.Jia
 * @date 2022/11/23
 */
@Component
public class TableBeanFactoryProcessor implements ApplicationContextAware, InitializingBean {

    private ApplicationContext applicationContext;

    @Override
    public void afterPropertiesSet() throws Exception {
        ObjectMappedFactory objectMappedFactory = applicationContext.getBean(ObjectMappedFactory.class);
        HbaseTemplate hbaseTemplate = applicationContext.getBean(HbaseTemplate.class);
        if (ObjectUtil.isAllNotEmpty(objectMappedFactory, hbaseTemplate)) {
            ConcurrentHashMap<String, ObjectMappedStatement> mappedStatements = (ConcurrentHashMap<String, ObjectMappedStatement>) ReflectUtil.getFieldValue(objectMappedFactory, "mappedStatements");
            mappedStatements.forEach((key, value) -> hbaseTemplate.createTable(value.getTableName()));
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
