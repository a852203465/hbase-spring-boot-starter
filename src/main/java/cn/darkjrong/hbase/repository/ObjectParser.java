package cn.darkjrong.hbase.repository;

import cn.darkjrong.hbase.common.enums.ExceptionEnum;
import cn.darkjrong.hbase.common.utils.HbaseUtils;
import cn.darkjrong.hbase.common.config.ObjectMappedFactory;
import cn.darkjrong.hbase.common.domain.ObjectMappedStatement;
import cn.darkjrong.hbase.common.domain.ObjectProperty;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.ReflectUtil;
import lombok.AllArgsConstructor;
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
@AllArgsConstructor
public class ObjectParser {

    private final ObjectMappedFactory objectMappedFactory;

    /**
     * 解析
     *
     * @param clazz  clazz
     * @param result 结果
     * @return {@link T}
     */
    public <T> T parse(Class<T> clazz, Result result) {
        ObjectMappedStatement mappedStatement = objectMappedFactory.getStatement(clazz.getName());
        Assert.notNull(mappedStatement, ExceptionEnum.getException(ExceptionEnum.MAPPED, clazz.getName()));
        T instance = ReflectUtil.newInstance(clazz);
        Map<String, ObjectProperty> properties = mappedStatement.getProperties();
        for(Cell cell : result.rawCells()) {
            String qualifier = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
            Object value = HbaseUtils.getValue(properties.get(qualifier).getType(), cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            ReflectUtil.setFieldValue(instance, qualifier, value);
        }
        return instance;
    }








}
