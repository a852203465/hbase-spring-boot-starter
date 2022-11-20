package cn.darkjrong.hbase.common.annotation;

import java.lang.annotation.*;

/**
 * 列名
 *
 * @author Rong.Jia
 * @date 2022/11/20
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ColumnName {

    /**
     * 字段名
     *
     * @return {@link String}
     */
    String value();




}
