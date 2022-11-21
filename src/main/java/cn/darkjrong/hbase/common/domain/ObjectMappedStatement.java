package cn.darkjrong.hbase.common.domain;

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
     * 表名字节数组
     */
    private byte[] tableNameBytes;

    /**
     * 列族名
     */
    private String columnFamily;

    /**
     * 列族名节数组
     */
    private byte[] columnFamilyBytes;

    /**
     * 行数
     */
    private byte[] rowKey;

    /**
     * 属性集合, key: 字段名, value: 字段属性
     */
    private Map<String, ObjectProperty> properties;



}
