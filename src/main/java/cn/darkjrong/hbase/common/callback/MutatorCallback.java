package cn.darkjrong.hbase.common.callback;

import org.apache.hadoop.hbase.client.BufferedMutator;


/**
 * Hbase 删除，修改回调
 *
 * @param <T> 实体泛型
 * @author Rong.Jia
 * @date 2022/11/20
 */
public interface MutatorCallback<T> {

    /**
     * 删除，修改
     *
     * @param mutator 突变
     * @return {@link T}
     * @throws Throwable 异常
     */
    T doInMutator(BufferedMutator mutator) throws Throwable;

}
