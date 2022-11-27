package cn.darkjrong.hbase;

import cn.darkjrong.hbase.domain.ObjectMappedStatement;
import cn.darkjrong.hbase.domain.ObjectProperty;
import cn.darkjrong.hbase.enums.HbaseExceptionEnum;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.*;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;

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
            Map<String, ObjectProperty> columns = statement.getColumns();

            for (Map.Entry<String, ObjectProperty> entry : columns.entrySet()) {
                Object value = getValue(entry.getValue().getType(), result.getValue(statement.getColumnFamilyBytes(), toBytes(entry.getKey())));
                ReflectUtil.setFieldValue(instance, entry.getKey(), value);
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
            return StrUtil.bytes((String) serializable, CharsetUtil.UTF_8);
        }
    }

    /**
     *将{@link Object}转换为 {@link byte[]}
     * @return {@link byte[]}
     */
    public static byte[] toBytes(Object value) {
        Class<?> tClass = value.getClass();
        if (tClass.equals(String.class)) {
            return toBytes((String) value);
        }else if (tClass.equals(Integer.class)) {
            return ByteUtil.intToBytes((Integer) value);
        }else if (tClass.equals(Long.class)) {
            return ByteUtil.longToBytes((Long) value);
        }else if (tClass.equals(Double.class)) {
            return ByteUtil.doubleToBytes((Double) value);
        }else if (tClass.equals(Float.class)) {
            return ByteUtil.floatToBytes((Float) value);
        }else if (tClass.equals(BigDecimal.class)) {
            return ByteUtil.numberToBytes((BigDecimal)value);
        }else if (tClass.equals(Boolean.class)) {
            return new byte[]{(byte)((Boolean)value ? -1 : 0)};
        }else if (tClass.equals(Short.class)) {
            return ByteUtil.shortToBytes((Short) value);
        }else {
            return JSON.toJSONBytes(value);
        }
    }

    /**
     * 将{@link byte[]}转换为 {@link String}
     * @param bytes {@link String}
     * @return {@link byte[]}
     */
    public static String toStr(byte[] bytes) {
        return StrUtil.str(bytes, CharsetUtil.UTF_8);
    }

    /**
     * 获取值
     *
     * @param tClass t类
     * @param value   数据
     * @return {@link Object}
     */
    public static Object getValue(Class<?> tClass, byte[] value) {
        if (tClass.equals(String.class)) {
            return toStr(value);
        }else if (tClass.equals(Integer.class)) {
            return ByteUtil.bytesToInt(value);
        }else if (tClass.equals(Long.class)) {
            return ByteUtil.bytesToLong(value);
        }else if (tClass.equals(Double.class)) {
            return ByteUtil.bytesToDouble(value);
        }else if (tClass.equals(Float.class)) {
            return ByteUtil.bytesToFloat(value);
        }else if (tClass.equals(BigDecimal.class)) {
            return ByteUtil.bytesToNumber(value, BigDecimal.class, ByteUtil.DEFAULT_ORDER);
        }else if (tClass.equals(Boolean.class)) {
            if (value.length != 1) {
                throw new IllegalArgumentException("Array has wrong size: " + value.length);
            } else {
                return value[0] != 0;
            }
        }else if (tClass.equals(Short.class)) {
            return ByteUtil.bytesToShort(value);
        }else {
            return JSON.parseObject(value, tClass);
        }
    }





























}
