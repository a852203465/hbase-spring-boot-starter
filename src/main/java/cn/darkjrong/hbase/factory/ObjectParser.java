package cn.darkjrong.hbase.factory;

import cn.darkjrong.hbase.common.domain.ObjectMappedStatement;
import cn.darkjrong.hbase.common.domain.ObjectProperty;
import cn.darkjrong.hbase.HbaseExceptionEnum;
import cn.darkjrong.hbase.HbaseUtils;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.ReflectUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * 解析器对象
 *
 * @author Rong.Jia
 * @date 2022/11/21
 */
public class ObjectParser {

    /**
     * 解析
     *
     * @param clazz  clazz
     * @param result 结果
     * @return {@link T}
     */
    public static <T> T parse(Class<T> clazz, Result result, ObjectMappedStatement statement) {
        Assert.notNull(statement, HbaseExceptionEnum.getException(HbaseExceptionEnum.MAPPED, clazz.getName()));
        T instance = ReflectUtil.newInstance(clazz);
        Map<String, ObjectProperty> properties = statement.getColumns();
        for(Cell cell : result.rawCells()) {
            String qualifier = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
            Object value = HbaseUtils.getValue(properties.get(qualifier).getType(), cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            ReflectUtil.setFieldValue(instance, qualifier, value);
        }
        return instance;
    }








}
