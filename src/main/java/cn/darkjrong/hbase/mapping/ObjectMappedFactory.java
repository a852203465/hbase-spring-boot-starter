package cn.darkjrong.hbase.mapping;

import cn.hutool.core.map.MapUtil;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 对象映射工厂
 *
 * @author Rong.Jia
 * @date 2022/11/20
 */
@Component
public class ObjectMappedFactory {

    /**
     * 对象映射关系
     *  key: 对象全限定名, value: 映射关系
     */
    private final ConcurrentHashMap<String, ObjectMappedStatement> mappedStatements = MapUtil.newConcurrentHashMap();

    /**
     * 添加映射
     *
     * @param id       对象全限定名
     * @param statement 映射
     */
    public void addStatement(String id, ObjectMappedStatement statement) {
        mappedStatements.put(id, statement);
    }

    /**
     * 获取映射
     *
     * @param id 对象全限定名
     * @return {@link ObjectMappedStatement}
     */
    public ObjectMappedStatement getStatement(String id) {
        return mappedStatements.get(id);
    }














}
