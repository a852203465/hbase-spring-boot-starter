package cn.darkjrong.hbase.keygen;

/**
 * 主键策略
 *
 * @author Rong.Jia
 * @date 2022/11/22
 */
public abstract class AbstractKeyGenerator implements RowKeyGenerator {

    /**
     * 默认id主键赋值
     * @param paramObj  参数对象
     */
    protected abstract void defaultGeneratorKey(Object paramObj);


}
