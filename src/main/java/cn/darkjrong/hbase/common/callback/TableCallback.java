package cn.darkjrong.hbase.common.callback;

import org.apache.hadoop.hbase.client.Table;

/**
 * 表 回调接口
 *
 * @param <T> 泛型
 * @author Rong.Jia
 * @date 2022/11/19
 */
public interface TableCallback<T> {

    /**
     * 操作表
     *
     * @param table 表
     * @return {@link T}
     * @throws Exception 异常
     */
    T doInTable(Table table) throws Exception;




}
