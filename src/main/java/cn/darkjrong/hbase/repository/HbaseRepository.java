package cn.darkjrong.hbase.repository;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * hbase 持久层
 *
 * @param <T> 实体类泛型
 * @param <ID> 主键类型泛型
 * @author Rong.Jia
 * @date 2022/11/27
 */
public interface HbaseRepository<T, ID extends Serializable> {

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
     * @param id id
     * @return {@link Boolean}
     */
    Boolean deleteById(ID id);



}
