package cn.darkjrong.hbase.keygen;

import cn.darkjrong.hbase.enums.IdType;

import java.lang.reflect.Field;

/**
 * 行键生成器
 *
 * @author Rong.Jia
 * @date 2022/11/22
 */
public interface RowKeyGenerator {

    /**
     * 主键赋值操作
     * @param field 字段
     * @param paramObj 对象
     */
    void postProcess(Field field, Object paramObj);

    /**
     * 返回主键策略类型
     * @return  策略类型
     */
    IdType getType();

}
