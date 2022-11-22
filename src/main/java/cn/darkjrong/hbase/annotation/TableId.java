package cn.darkjrong.hbase.annotation;


import cn.darkjrong.hbase.enums.IdType;

import java.lang.annotation.*;

/**
 * 表主键
 *
 * @author Rong.Jia
 * @date 2022/11/20
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TableId {

    IdType value() default IdType.ASSIGN_ID;

}
