package cn.darkjrong.hbase.service;

import java.util.List;

/**
 * hbase 公共Service
 *
 * @author Rong.Jia
 * @date 2022/11/20
 */
public interface HbaseService<T, ID> {

    /**
     * 保存
     *
     * @param entity 实体
     */
    void save(T entity);

    List<T> findAll();

//    <S extends T> Iterable<S> saveAll(Iterable<S> entities);
//
//    Optional<T> findById(ID id);
//
//    boolean existsById(ID id);
//

//
//    Iterable<T> findAllById(Iterable<ID> ids);
//
//    long count();
//
//    void deleteById(ID id);
//
//    void delete(T entity);
//
//    void deleteAllById(Iterable<? extends ID> ids);
//
//    void deleteAll(Iterable<? extends T> entities);
//
//    void deleteAll();



}
