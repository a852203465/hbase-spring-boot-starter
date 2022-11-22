package cn.darkjrong.hbase.annotation;


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


}
