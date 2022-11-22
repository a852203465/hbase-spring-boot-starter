package cn.darkjrong.hbase;

import cn.darkjrong.hbase.domain.ObjectMappedStatement;
import cn.darkjrong.hbase.domain.ObjectProperty;
import cn.darkjrong.hbase.enums.HbaseExceptionEnum;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;

/**
 * hbase 工具类
 *
 * @author Rong.Jia
 * @date 2022/11/19
 */
@Slf4j
public class HbaseUtils {

    /**
     * 关闭
     *
     * @param scanner 结果扫描器
     */
    public static void close(ResultScanner scanner) {
        if (ObjectUtil.isNotNull(scanner)) {
            scanner.close();
        }
    }

    /**
     * 关闭
     *
     * @param table 表
     */
    public static void close(Table table) {
        if (ObjectUtil.isNotNull(table)) {
            try {
                table.close();
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * 关闭
     *
     * @param mutator mutator资源
     */
    public static void close(BufferedMutator mutator) {
        if (ObjectUtil.isNotNull(mutator)) {
            try {
                mutator.flush();
                mutator.close();
            } catch (Exception e) {
                log.error("hbase mutator资源释放失败 {}", e.getMessage());
            }
        }
    }

    /**
     * 获取值
     *
     * @param tClass t类
     * @param data   数据
     * @return {@link Object}
     */
    public static Object getValue(Class<?> tClass, byte[] data) {
        if (tClass.equals(String.class)) {
            return toStr(data);
        }else if (tClass.equals(int.class) || tClass.equals(Integer.class)) {
            return Convert.toInt(data);
        }else if (tClass.equals(Long.class) || tClass.equals(long.class)) {
            return Convert.toLong(data);
        }else if (tClass.equals(double.class) || tClass.equals(Double.class)) {
            return Convert.toDouble(data);
        }else if (tClass.equals(float.class) || tClass.equals(Float.class)) {
            return Convert.toFloat(data);
        }else if (tClass.equals(BigDecimal.class)) {
            return Convert.toBigDecimal(data);
        }else if (tClass.equals(Boolean.class) || tClass.equals(boolean.class)) {
            return Convert.toBool(data);
        }else if (tClass.equals(Short.class) || tClass.equals(short.class)) {
            return Convert.toShort(data);
        }else {
            return toStr(data);
        }
    }

    /**
     * 获取值
     *
     * @param tClass t类
     * @param data   数据
     * @param offset 偏移量
     * @param length 长度
     * @return {@link Object}
     */
    public static Object getValue(Class<?> tClass, byte[] data, int offset, int length) {
        if (tClass.equals(String.class)) {
            return Bytes.toString(data, offset, length);
        }else if (tClass.equals(int.class) || tClass.equals(Integer.class)) {
            return Convert.toInt(data);
        }else if (tClass.equals(Long.class) || tClass.equals(long.class)) {
            return Convert.toLong(data);
        }else if (tClass.equals(double.class) || tClass.equals(Double.class)) {
            return Convert.toDouble(data);
        }else if (tClass.equals(float.class) || tClass.equals(Float.class)) {
            return Convert.toFloat(data);
        }else if (tClass.equals(BigDecimal.class)) {
            return Bytes.toBigDecimal(data, offset, length);
        }else if (tClass.equals(Boolean.class) || tClass.equals(boolean.class)) {
            return Convert.toBool(data);
        }else if (tClass.equals(Short.class) || tClass.equals(short.class)) {
            return Convert.toShort(data);
        }else {
            return Bytes.toString(data, offset, length);
        }
    }

    /**
     * 对象解析
     *
     * @param clazz     clazz
     * @param result    结果
     * @param statement 声明
     * @return {@link T}
     */
    public static <T> T objectParse(Class<T> clazz, Result result, ObjectMappedStatement statement) {
        Assert.notNull(statement, HbaseExceptionEnum.getException(HbaseExceptionEnum.MAPPED, clazz.getName()));
        if (ArrayUtil.isNotEmpty(result.rawCells())) {
            T instance = ReflectUtil.newInstance(clazz);
            Map<String, ObjectProperty> properties = statement.getColumns();

            for (Map.Entry<String, ObjectProperty> entry : properties.entrySet()) {
                System.out.println(toStr(result.getValue(statement.getColumnFamilyBytes(), toBytes(entry.getKey()))));
            }

            for(Cell cell : result.rawCells()) {
                String qualifier = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                Object value = HbaseUtils.getValue(properties.get(qualifier).getType(), cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                ReflectUtil.setFieldValue(instance, qualifier, value);
            }
            return instance;
        }
        return null;
    }

    /**
     *将{@link Serializable}转换为 {@link byte[]}
     * @param serializable {@link Serializable}
     * @return {@link byte[]}
     */
    public static byte[] toBytes(Serializable serializable) {
        if (serializable instanceof Number) {
            return ByteUtil.numberToBytes((Number) serializable);
        } else {
            return StrUtil.bytes((String) serializable);
        }
    }

    /**
     *将{@link Object}转换为 {@link byte[]}
     * @param clazz {@link Class<?>}  key的类型
     * @return {@link byte[]}
     */
    public static byte[] toBytes(Class<?> clazz, Object key) {
        if (Number.class.isAssignableFrom(clazz)) {
            return ByteUtil.numberToBytes((Number) key);
        } else if (String.class.isAssignableFrom(clazz)){
            return StrUtil.bytes((String) key);
        }else {
            return Convert.toPrimitiveByteArray(key);
        }
    }

    /**
     *将{@link String}转换为 {@link byte[]}
     * @param value {@link String}
     * @return {@link byte[]}
     */
    public static byte[] toBytes(String value) {
       return StrUtil.bytes(value);
    }

    /**
     * 将{@link byte[]}转换为 {@link String}
     * @param bytes {@link String}
     * @return {@link byte[]}
     */
    public static String toStr(byte[] bytes) {
        return new String(bytes);
    }






























}
