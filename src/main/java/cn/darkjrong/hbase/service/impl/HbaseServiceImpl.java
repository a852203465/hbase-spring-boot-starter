package cn.darkjrong.hbase.service.impl;

import cn.darkjrong.hbase.repository.HbaseRepository;
import cn.darkjrong.hbase.service.HbaseService;
import org.springframework.beans.factory.annotation.Autowired;

import java.lang.reflect.ParameterizedType;
import java.util.List;

/**
 * hbase 公共Service实现类
 *
 * @author Rong.Jia
 * @date 2022/11/20
 */
public class HbaseServiceImpl<T, ID> implements HbaseService<T, ID> {

    @Autowired
    private HbaseRepository<T, ID> hbaseRepository;

    protected Class<T> currentTargetClass() {
        ParameterizedType parameterizedType = (ParameterizedType) this.getClass().getGenericSuperclass();
        return (Class<T>) parameterizedType.getActualTypeArguments()[0];
    }

    @Override
    public <S extends T> S save(S entity) {
       return hbaseRepository.save(entity);
    }

    @Override
    public List<T> findAll() {
//        return hbaseTemplate.find(currentTable(), new RowMapper<T>() {
//            @Override
//            public T mapRow(Result result, int rowNum) {
//                return objectParser.parse(currentTargetClass(), result);
//            }
//        });
        return null;
    }
}
