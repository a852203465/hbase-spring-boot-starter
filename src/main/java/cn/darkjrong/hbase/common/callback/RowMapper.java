package cn.darkjrong.hbase.common.callback;

import cn.darkjrong.hbase.HbaseException;
import org.apache.hadoop.hbase.client.Result;

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
}
