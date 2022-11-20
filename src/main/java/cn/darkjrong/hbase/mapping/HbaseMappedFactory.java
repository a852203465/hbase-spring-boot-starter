package cn.darkjrong.hbase.mapping;

import cn.hutool.core.map.MapUtil;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

/**
 * hbase映射工厂
 *
 * @author Rong.Jia
 * @date 2022/11/20
 */
@Data
public class HbaseMappedFactory implements Serializable {

    private static final long serialVersionUID = 4990718430270140622L;

    /**
     * 对象映射关系
     *  key: 对象全限定名, value: 映射关系
     */
    private static Map<String, ObjectMappedStatement> mappedStatements = MapUtil.newHashMap();

    /**
     * 添加映射
     *
     * @param id       对象全限定名
     * @param statement 映射
     */
    public static synchronized void addStatement(String id, ObjectMappedStatement statement) {
        mappedStatements.put(id, statement);
    }

    /**
     * 获取映射
     *
     * @param id 对象全限定名
     * @return {@link ObjectMappedStatement}
     */
    public static ObjectMappedStatement getStatement(String id) {
        return mappedStatements.get(id);
    }

    /**
     * 获取映射
     *
     * @return {@link Map}<{@link String}, {@link ObjectMappedStatement}>
     */
    public static Map<String, ObjectMappedStatement> getStatements() {
        return mappedStatements;
    }












}
