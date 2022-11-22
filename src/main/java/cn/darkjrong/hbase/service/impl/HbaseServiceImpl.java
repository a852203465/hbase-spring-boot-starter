package cn.darkjrong.hbase.service.impl;

import cn.darkjrong.hbase.HbaseTemplate;
import cn.darkjrong.hbase.HbaseUtils;
import cn.darkjrong.hbase.callback.RowMapper;
import cn.darkjrong.hbase.domain.ObjectMappedStatement;
import cn.darkjrong.hbase.factory.ObjectMappedFactory;
import cn.darkjrong.hbase.factory.RowKeyGeneratorFactory;
import cn.darkjrong.hbase.service.HbaseService;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ReflectUtil;
import cn.hutool.core.util.TypeUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * hbase 公共Service实现类
 *
 * @author Rong.Jia
 * @date 2022/11/20
 */
public class HbaseServiceImpl<T, ID extends Serializable> implements HbaseService<T, ID> {

    protected Class<T> targetClass = this.currentTargetClass();
    protected Class<ID> keyClass = this.currentKeyClass();


    @Autowired
    protected HbaseTemplate hbaseTemplate;

    @Autowired
    private ObjectMappedFactory objectMappedFactory;

    @Autowired
    private RowKeyGeneratorFactory rowKeyGeneratorFactory;

    protected Class<T> currentTargetClass() {
        return (Class<T>) TypeUtil.getTypeArgument(this.getClass());
    }

    protected Class<ID> currentKeyClass() {
        return (Class<ID>) TypeUtil.getTypeArgument(this.getClass(), 1);
    }

    protected ObjectMappedStatement currentStatement() {
        return objectMappedFactory.getStatement(targetClass.getName());
    }

    @Override
    public <S extends T> S save(S entity) {
        ObjectMappedStatement mappedStatement = currentStatement();
        List<Mutation> mutations = new ArrayList<>();
        rowKeyGeneratorFactory.doHandler(mappedStatement.getIdType(), entity, mappedStatement.getTableId());
        Object fieldValue = ReflectUtil.getFieldValue(entity, mappedStatement.getTableId());
        Put put = new Put(HbaseUtils.toBytes(currentKeyClass(), fieldValue));
        mappedStatement.getColumns()
                .forEach((key, value) ->
                        put.addColumn(mappedStatement.getColumnFamilyBytes(),
                                value.getColumnBytes(),
                                HbaseUtils.toBytes(value.getType(), ReflectUtil.getFieldValue(entity, value.getField()))));
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

    @Override
    public <S extends T> Iterable<S> saveAll(Iterable<S> entities) {
        if (CollectionUtil.isNotEmpty(entities)) {
            List<S> collections = new ArrayList<>();
            entities.forEach(a-> collections.add(this.save(a)));
           return collections;
        }
        return Collections.emptyList();
    }

    @Override
    public Optional<T> findById(ID id) {
        ObjectMappedStatement statement = currentStatement();
        return Optional.of(hbaseTemplate.get(statement.getTableName(), id, statement.getColumnFamily(), new RowMapper<T>() {
            @Override
            public T mapRow(Result result, int rowNum) {
                return HbaseUtils.objectParse(targetClass, result, currentStatement());
            }
        }));
    }

    @Override
    public boolean existsById(ID id) {
        return false;
    }
}
