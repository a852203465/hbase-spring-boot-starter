package cn.darkjrong.hbase.common.annotation;

import cn.darkjrong.hbase.factory.ObjectMappedRegistrar;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * 映射扫描
 *
 * @author Rong.Jia
 * @date 2022/11/20
 */
@Documented
@Target({ElementType.TYPE})
@Import({ObjectMappedRegistrar.class})
@Retention(RetentionPolicy.RUNTIME)
public @interface MappedScan {

    /**
     * 扫描包数组
     * @return 扫描包
     */
    String[] value() default {};

    /**
     * 扫描包数组
     * @return 扫描包
     */
    String[] basePackages() default {};

}
