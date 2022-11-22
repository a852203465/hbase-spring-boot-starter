package cn.darkjrong.hbase;

import cn.darkjrong.hbase.domain.ObjectMappedStatement;
import cn.darkjrong.hbase.domain.ObjectProperty;
import cn.darkjrong.hbase.enums.HbaseExceptionEnum;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.ReflectUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
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
        if (ObjectUtil.isNotNull(scanner)) scanner.close();
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
            return Bytes.toString(data);
        }else if (tClass.equals(int.class) || tClass.equals(Integer.class)) {
            return Bytes.toInt(data);
        }else if (tClass.equals(double.class) || tClass.equals(Double.class)) {
            return Bytes.toDouble(data);
        }else if (tClass.equals(float.class) || tClass.equals(Float.class)) {
            return Bytes.toFloat(data);
        }else if (tClass.equals(BigDecimal.class)) {
            return Bytes.toBigDecimal(data);
        }else if (tClass.equals(Boolean.class) || tClass.equals(boolean.class)) {
            return Bytes.toBoolean(data);
        }else if (tClass.equals(Short.class) || tClass.equals(short.class)) {
            return Bytes.toShort(data);
        }else {
            return Bytes.toString(data);
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
            return Bytes.toInt(data);
        }else if (tClass.equals(double.class) || tClass.equals(Double.class)) {
            return Bytes.toDouble(data);
        }else if (tClass.equals(float.class) || tClass.equals(Float.class)) {
            return Bytes.toFloat(data);
        }else if (tClass.equals(BigDecimal.class)) {
            return Bytes.toBigDecimal(data, offset, length);
        }else if (tClass.equals(Boolean.class) || tClass.equals(boolean.class)) {
            return Bytes.toBoolean(data);
        }else if (tClass.equals(Short.class) || tClass.equals(short.class)) {
            return Bytes.toShort(data);
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
        T instance = ReflectUtil.newInstance(clazz);
        Map<String, ObjectProperty> properties = statement.getColumns();
        for(Cell cell : result.rawCells()) {
            System.out.println(cell.toString());
            String qualifier = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
            Object value = HbaseUtils.getValue(properties.get(qualifier).getType(), cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            ReflectUtil.setFieldValue(instance, qualifier, value);
        }
        return instance;
    }

















}
