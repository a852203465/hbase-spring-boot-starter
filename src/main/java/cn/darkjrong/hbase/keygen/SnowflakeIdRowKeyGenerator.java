package cn.darkjrong.hbase.keygen;

import cn.darkjrong.hbase.HbaseConstant;
import cn.darkjrong.hbase.enums.IdType;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.ReflectUtil;

import java.lang.reflect.Field;

/**
 * 雪花算法主键策略
 *
 * @author Rong.Jia
 * @date 2022/11/22
 */
public class SnowflakeIdRowKeyGenerator extends AbstractRowKeyGenerator {

    @Override
    public void postProcess(Field field, Object paramObj) {
        if (field == null) {
            defaultGeneratorKey(paramObj);
            return;
        }
        if (Number.class.isAssignableFrom(field.getType())) {
            ReflectUtil.setFieldValue(paramObj, field, IdUtil.getSnowflakeNextId());
        }else if (field.getType().isAssignableFrom(String.class)) {
            ReflectUtil.setFieldValue(paramObj, field, IdUtil.getSnowflakeNextIdStr());
        }
    }

    @Override
    public IdType getType() {
        return IdType.ASSIGN_ID;
    }

    @Override
    protected void defaultGeneratorKey(Object paramObj) {
        Field field = ReflectUtil.getField(paramObj.getClass(), HbaseConstant.ID);
        if (ReflectUtil.getFieldValue(paramObj, field) == null) {
            if (Number.class.isAssignableFrom(field.getType())) {
                ReflectUtil.setFieldValue(paramObj, field, IdUtil.getSnowflakeNextId());
            }else if (field.getType().isAssignableFrom(String.class)) {
                ReflectUtil.setFieldValue(paramObj, field, IdUtil.getSnowflakeNextIdStr());
            }
        }
    }
}
