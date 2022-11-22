package cn.darkjrong.hbase.factory;

import cn.darkjrong.hbase.enums.IdType;
import cn.darkjrong.hbase.keygen.RowKeyGenerator;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 行键生成器工厂
 *
 * @author Rong.Jia
 * @date 2022/11/22
 */
public class RowKeyGeneratorFactory implements InitializingBean, ApplicationContextAware {

    private ApplicationContext applicationContext;

    private final Map<IdType, RowKeyGenerator> idHandlerMap = new ConcurrentHashMap<>();

    @Override
    public void afterPropertiesSet() {
        Map<String, RowKeyGenerator> idHandlers = applicationContext.getBeansOfType(RowKeyGenerator.class);
        idHandlers.forEach((k, v) -> idHandlerMap.put(v.getType(), v));
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    /**
     * 对应类型逻辑处理
     *
     * @param type     类型
     * @param paramObj 参数对象
     * @param field    主键列属性
     */
    public void doHandler(IdType type, Object paramObj, Field field) {
        RowKeyGenerator keyGenerator = idHandlerMap.get(type);
        Optional.ofNullable(keyGenerator).orElseThrow(() -> new IllegalArgumentException(String.format("不存在%s类型主键生成策略", type)));
        keyGenerator.postProcess(field, paramObj);
    }
}
