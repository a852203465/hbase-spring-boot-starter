package cn.darkjrong.hbase.service.impl;

import cn.darkjrong.hbase.HbaseTemplate;
import cn.darkjrong.hbase.HbaseUtils;
import cn.darkjrong.hbase.callback.RowMapper;
import cn.darkjrong.hbase.domain.ObjectMappedStatement;
import cn.darkjrong.hbase.factory.ObjectMappedFactory;
import cn.darkjrong.hbase.factory.RowKeyGeneratorFactory;
import cn.darkjrong.hbase.service.HbaseService;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.ReflectUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;

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

    protected Class<T> targetClass = this.currentTargetClass();
    protected Class<ID> keyClass = this.currentKeyType();

    @Autowired
    protected HbaseTemplate hbaseTemplate;

    @Autowired
    private ObjectMappedFactory objectMappedFactory;

    @Autowired
    private RowKeyGeneratorFactory rowKeyGeneratorFactory;

    protected Class<T> currentTargetClass() {
        ParameterizedType parameterizedType = (ParameterizedType) this.getClass().getGenericSuperclass();
        return (Class<T>) parameterizedType.getActualTypeArguments()[0];
    }

    protected Class<ID> currentKeyType() {
        ParameterizedType parameterizedType = (ParameterizedType) this.getClass().getGenericSuperclass();
        return (Class<ID>) parameterizedType.getActualTypeArguments()[1];
    }

    protected ObjectMappedStatement currentStatement() {
        return objectMappedFactory.getStatement(targetClass.getName());
    }

    @Override
    public <S extends T> S save(S entity) {
        ObjectMappedStatement mappedStatement = currentStatement();
        List<Mutation> mutations = new ArrayList<>();
        rowKeyGeneratorFactory.doHandler(mappedStatement.getIdType(), entity, mappedStatement.getTableId());
        Put put = new Put(Bytes.toBytes(Convert.toStr(ReflectUtil.getFieldValue(entity, mappedStatement.getTableId()))));
        mappedStatement.getColumns()
                .forEach((key, value) ->
                        put.addColumn(mappedStatement.getColumnFamilyBytes(),
                                value.getColumnBytes(),
                                Bytes.toBytes(Convert.toStr(ReflectUtil.getFieldValue(entity, value.getField())))));
        mutations.add(put);
        hbaseTemplate.saveOrUpdate(mappedStatement.getTableName(), mutations);
        return entity;
    }

    @Override
    public List<T> findAll() {
        return hbaseTemplate.find(currentStatement().getTableName(), new RowMapper<T>() {
            @Override
            public T mapRow(Result result, int rowNum) {
                return HbaseUtils.objectParse(targetClass, result, currentStatement());
            }
        });
    }
}
