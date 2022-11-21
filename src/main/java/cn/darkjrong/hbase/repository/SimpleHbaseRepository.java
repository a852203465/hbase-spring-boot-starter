package cn.darkjrong.hbase.repository;

import cn.darkjrong.hbase.HbaseTemplate;
import cn.darkjrong.hbase.common.config.ObjectMappedFactory;
import cn.darkjrong.hbase.common.domain.ObjectMappedStatement;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.ReflectUtil;
import lombok.AllArgsConstructor;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;

/**
 * 操作Hbase接口实现类
 *
 * @author Rong.Jia
 * @date 2022/11/21
 */
@Component
@AllArgsConstructor
public class SimpleHbaseRepository<T, ID> implements HbaseRepository<T, ID> {

    private final HbaseTemplate hbaseTemplate;
    private final ObjectParser objectParser;
    private final ObjectMappedFactory factory;

    protected Class<T> currentTargetClass() {
        ParameterizedType parameterizedType = (ParameterizedType) this.getClass().getGenericSuperclass();
        return (Class<T>) parameterizedType.getActualTypeArguments()[0];
    }

    protected ObjectMappedStatement currentStatement() {
       return factory.getStatement(currentTargetClass().getName());
    }

    @Override
    public <S extends T> S save(S entity) {
        ObjectMappedStatement mappedStatement = currentStatement();
        List<Mutation> mutations = new ArrayList<>();
        Put put = new Put(mappedStatement.getRowKey());
        mappedStatement.getProperties()
                .forEach((key, value) ->
                put.addColumn(mappedStatement.getColumnFamilyBytes(),
                value.getColumnBytes(),
                Bytes.toBytes(Convert.toStr(ReflectUtil.getFieldValue(entity, value.getField())))));
        mutations.add(put);
        hbaseTemplate.saveOrUpdate(mappedStatement.getTableName(), mutations);
        return entity;
    }













}
