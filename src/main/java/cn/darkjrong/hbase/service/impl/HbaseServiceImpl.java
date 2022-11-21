package cn.darkjrong.hbase.service.impl;

import cn.darkjrong.hbase.HbaseTemplate;
import cn.darkjrong.hbase.common.annotation.TableName;
import cn.darkjrong.hbase.common.callback.RowMapper;
import cn.darkjrong.hbase.common.constants.HbaseConstant;
import cn.darkjrong.hbase.common.enums.ExceptionEnum;
import cn.darkjrong.hbase.common.exceptions.HbaseException;
import cn.darkjrong.hbase.common.utils.HbaseUtils;
import cn.darkjrong.hbase.service.HbaseService;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.ReflectUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;

/**
 * hbase 公共Service实现类
 *
 * @author Rong.Jia
 * @date 2022/11/20
 */
public class HbaseServiceImpl<T, ID> implements HbaseService<T, ID> {

    @Autowired
    private HbaseTemplate hbaseTemplate;

    private Class<T> currentTargetClass() {
        ParameterizedType parameterizedType = (ParameterizedType) this.getClass().getGenericSuperclass();
        return (Class<T>) parameterizedType.getActualTypeArguments()[0];
    }

    private String currentTable() {
        TableName annotation = currentTargetClass().getAnnotation(TableName.class);
        Assert.notNull(annotation, ExceptionEnum.TABLE_NAME_NOT_FOUND.getValue());
        return annotation.value();
    }

    @Override
    public void save(T entity) {
        List<Field> fields = CollectionUtil.newArrayList(ReflectUtil.getFields(currentTargetClass()));
        List<Mutation> mutations = new ArrayList<>();
        Put put = new Put(Bytes.toBytes(fields.size()));
        for (Field field : fields) {
            put.addColumn(Bytes.toBytes(HbaseConstant.DEFAULT_COLUMN_FAMILY),
                    Bytes.toBytes(field.getName()), Bytes.toBytes(Convert.toStr(ReflectUtil.getFieldValue(entity, field))));
        }
        mutations.add(put);

        hbaseTemplate.saveOrUpdate(currentTable(), mutations);
    }

    @Override
    public List<T> findAll() {
        return hbaseTemplate.find(currentTable(), new RowMapper<T>() {
            @Override
            public T mapRow(Result result, int rowNum) throws HbaseException {
                T instance = ReflectUtil.newInstance(currentTargetClass());
                for(Cell cell : result.rawCells()) {
                    String qualifier = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                    Object value = HbaseUtils.getValue(ReflectUtil.getField(currentTargetClass(), qualifier).getType(), cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    ReflectUtil.setFieldValue(instance, qualifier, value);
                }
                return instance;
            }
        });
    }
}
