package cn.darkjrong.hbase.service;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

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
     * @return {@link Boolean}
     */
    Boolean save(T entity);

    /**
     * 查询所有
     *
     * @return {@link List}<{@link T}>
     */
    List<T> findAll();

    /**
     * 根据ID查询
     *
     * @param id id
     * @return {@link T}
     */
    T findById(ID id);

    /**
     * 根据ID判断是否存在数据
     *
     * @param id 主键
     * @return {@link Boolean}
     */
    Boolean existsById(ID id);

    /**
     * 根据ID查询所有
     *
     * @param ids id
     * @return {@link List}<{@link T}>
     */
    List<T> findAllById(Set<ID> ids);

    /**
     * 根据ID删除
     *
     * @param id 主键
     * @return {@link Boolean}
     */
    Boolean deleteById(ID id);

    /**
     * 根据ID集合删除
     *
     * @param ids 主键
     * @return {@link Boolean}
     */
    Boolean deleteAllById(Set<ID> ids);


}
