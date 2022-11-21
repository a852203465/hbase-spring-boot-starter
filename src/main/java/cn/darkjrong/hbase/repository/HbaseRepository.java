package cn.darkjrong.hbase.repository;

import java.util.List;

/**
 * hbase 持久层接口
 *
 * @author Rong.Jia
 * @date 2022/11/21
 */
public interface HbaseRepository<T, ID> {

    /**
     * 新增
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























}
