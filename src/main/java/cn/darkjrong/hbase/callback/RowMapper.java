package cn.darkjrong.hbase.callback;

import cn.darkjrong.hbase.HbaseException;
import cn.hutool.core.collection.CollectionUtil;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;
import java.util.Optional;

/**
 * 行映射器
 *
 * @author Rong.Jia
 * @date 2022/11/19
 */
public interface RowMapper<T> {

    /**
     * 行处理
     *
     * @param result 结果
     * @param rowNum 行数
     * @return {@link T}
     * @throws HbaseException hbase异常
     */
    T mapRow(Result result, int rowNum) throws HbaseException;

    /**
     * 行处理
     *
     * @param results 结果集合
     * @return {@link List}<{@link T}>
     * @throws HbaseException hbase异常
     */
    default List<T> mapRow(Result[] results) throws HbaseException {
        List<T> rs = CollectionUtil.newArrayList();
        int rowNum = 0;
        for (Result result : results) {
            Optional.ofNullable(mapRow(result, rowNum++)).ifPresent(rs::add);
        }
        return rs;
    }
}
