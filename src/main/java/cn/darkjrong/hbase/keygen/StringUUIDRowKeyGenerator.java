package cn.darkjrong.hbase.keygen;

import cn.darkjrong.hbase.HbaseConstant;
import cn.darkjrong.hbase.enums.IdType;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.ReflectUtil;

import java.lang.reflect.Field;

/**
 * String类型主键赋值
 *
 * @author Rong.Jia
 * @date 2022/11/22
 */
public class StringUUIDRowKeyGenerator extends AbstractRowKeyGenerator {

    @Override
    public void postProcess(Field field, Object paramObj) {
        if (null == field) {
            defaultGeneratorKey(paramObj);
            return;
        }
        if (!String.class.isAssignableFrom(field.getType())) {
            throw new IllegalArgumentException("主键策略UUID==》对应主键属性类型必须为String");
        }
        ReflectUtil.setFieldValue(paramObj, field, IdUtil.fastSimpleUUID());
    }

    @Override
    public IdType getType() {
        return IdType.ASSIGN_UUID;
    }

    @Override
    protected void defaultGeneratorKey(Object paramObj) {
        Field field = ReflectUtil.getField(paramObj.getClass(), HbaseConstant.ID);
        if (ReflectUtil.getFieldValue(paramObj, field) == null) {
            if (!String.class.isAssignableFrom(field.getType())) {
                throw new IllegalArgumentException("主键策略UUID==》对应主键属性类型必须为String");
            }
            ReflectUtil.setFieldValue(paramObj, field, IdUtil.fastSimpleUUID());
        }
    }


}
