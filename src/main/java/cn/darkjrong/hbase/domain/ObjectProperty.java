package cn.darkjrong.hbase.domain;

import lombok.Data;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * 对象属性
 *
 * @author Rong.Jia
 * @date 2022/11/20
 */
@Data
public class ObjectProperty implements Serializable {

    private static final long serialVersionUID = -4899385441172512964L;

    /**
     * 字段
     */
    private Field field;

    /**
     * 列名
     */
    private String column;

    /**
     * 列名 字节数组
     */
    private byte[] columnBytes;

    /**
     * 字段类型
     */
    private Class<?> type;















}
