package cn.darkjrong.hbase.common.annotation;

import cn.darkjrong.hbase.common.constants.HbaseConstant;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

/**
 * 表名
 *
 * @author Rong.Jia
 * @date 2022/11/20
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TableName {

    /**
     * 表名
     *
     * @return {@link String}
     */
    @AliasFor("name")
    String value() default "";

    /**
     * 表名
     *
     * @return {@link String}
     */
    @AliasFor("value")
    String name() default "";

    /**
     * 列族名
     *
     * @return {@link String}
     */
    String columnFamily() default HbaseConstant.DEFAULT_COLUMN_FAMILY;



}
