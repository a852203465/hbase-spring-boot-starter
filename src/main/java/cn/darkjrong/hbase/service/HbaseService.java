package cn.darkjrong.hbase.service;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * hbase 公共Service
 *
 * @author Rong.Jia
 * @date 2022/11/20
 */
public interface HbaseService<T, ID extends Serializable> {

    /**
     * 添加
     *
     * @param entity 实体
     * @return {@link S}
     */
    <S extends T> S save(S entity);

    /**
     * 查询所有
     *
     * @return {@link List}<{@link T}>
     */
    List<T> findAll();

    /**
     * 保存所有
     *
     * @param entities 实体
     * @return {@link Iterable}<{@link S}>
     */
    <S extends T> Iterable<S> saveAll(Iterable<S> entities);

    /**
     * 根据ID查询
     *
     * @param id id
     * @return {@link Optional}<{@link T}>
     */
    Optional<T> findById(ID id);

    boolean existsById(ID id);



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
