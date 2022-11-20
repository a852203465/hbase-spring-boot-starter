package cn.darkjrong.hbase.mapping;

import lombok.Data;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Map;

/**
 * 对象映射语句
 *
 * @author Rong.Jia
 * @date 2022/11/20
 */
@Data
public class ObjectMappedStatement implements Serializable {

    private static final long serialVersionUID = -8075053584584266106L;

    /**
     * ID, 全限定类名
     */
    private String id;

    /**
     * 表主键字段
     */
    private Field tableId;

    /**
     * 表名
     */
    private String tableName;

    /**
     * 列族名
     */
    private String columnFamily;

    /**
     * 属性集合, key: 字段名, value: 字段属性
     */
    private Map<String, ObjectProperty> properties;



}
