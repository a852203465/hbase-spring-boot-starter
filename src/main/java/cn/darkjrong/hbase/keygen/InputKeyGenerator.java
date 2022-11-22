package cn.darkjrong.hbase.keygen;

import cn.darkjrong.hbase.enums.IdType;

import java.lang.reflect.Field;

/**
 * 自定义主键策略
 *
 * @author Rong.Jia
 * @date 2022/11/22
 */
public class InputKeyGenerator extends AbstractKeyGenerator {

    @Override
    public void postProcess(Field field, Object paramObj) {

    }

    @Override
    public IdType getType() {
        return IdType.INPUT;
    }

    @Override
    protected void defaultGeneratorKey(Object paramObj) {
    }
}
