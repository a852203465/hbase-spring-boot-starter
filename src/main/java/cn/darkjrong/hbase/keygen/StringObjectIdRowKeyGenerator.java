package cn.darkjrong.hbase.keygen;

import cn.darkjrong.hbase.HbaseConstant;
import cn.darkjrong.hbase.enums.IdType;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.ReflectUtil;

import java.lang.reflect.Field;

/**
 * 字符串ObjectId键生成器
 *
 * @author Rong.Jia
 * @date 2022/11/22
 */
public class StringObjectIdRowKeyGenerator extends AbstractRowKeyGenerator {

    @Override
    public void postProcess(Field field, Object paramObj) {
        if (null == field) {
            defaultGeneratorKey(paramObj);
            return;
        }
        if (!String.class.isAssignableFrom(field.getType())) {
            throw new IllegalArgumentException("主键策略ObjectId==》对应主键属性类型必须为String");
        }
        ReflectUtil.setFieldValue(paramObj, field, IdUtil.objectId());
    }

    @Override
    public IdType getType() {
        return IdType.ASSIGN_OBJECT_ID;
    }

    @Override
    protected void defaultGeneratorKey(Object paramObj) {
        Field field = ReflectUtil.getField(paramObj.getClass(), HbaseConstant.ID);
        if (ReflectUtil.getFieldValue(paramObj, field) == null) {
            if (!String.class.isAssignableFrom(field.getType())) {
                throw new IllegalArgumentException("主键策略ObjectId==》对应主键属性类型必须为String");
            }
            ReflectUtil.setFieldValue(paramObj, field, IdUtil.objectId());
        }
    }
}
