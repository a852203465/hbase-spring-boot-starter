package cn.darkjrong.hbase.common.configuration;

import cn.darkjrong.hbase.common.annotation.ColumnName;
import cn.darkjrong.hbase.common.annotation.MappedScan;
import cn.darkjrong.hbase.common.annotation.TableId;
import cn.darkjrong.hbase.common.annotation.TableName;
import cn.darkjrong.hbase.common.constants.QueryConstant;
import cn.darkjrong.hbase.common.enums.ExceptionEnum;
import cn.darkjrong.hbase.mapping.HbaseMappedFactory;
import cn.darkjrong.hbase.mapping.ObjectMappedStatement;
import cn.darkjrong.hbase.mapping.ObjectProperty;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.ClassUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.ReflectUtil;
import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 对象映射扫描注册器
 *
 * @author Rong.Jia
 * @date 2022/11/20
 */
@Slf4j
public class ObjectMappedScanner implements ImportBeanDefinitionRegistrar, ResourceLoaderAware, EnvironmentAware {

    /**
     * 资源加载器
     */
    private ResourceLoader resourceLoader;

    /**
     * 环境
     */
    private Environment environment;

    @Override
    public void registerBeanDefinitions(AnnotationMetadata metadata, BeanDefinitionRegistry registry) {
        // 创建scanner
        ClassPathScanningCandidateComponentProvider scanner = getScanner();
        scanner.setResourceLoader(resourceLoader);

        // 设置扫描器scanner扫描的过滤条件
        AnnotationTypeFilter annotationTypeFilter = new AnnotationTypeFilter(TableName.class);
        scanner.addIncludeFilter(annotationTypeFilter);

        // 获取指定要扫描的basePackages
        Set<String> basePackages = getBasePackages(metadata);

        // 遍历每一个basePackages
        for (String basePackage : basePackages) {
            // 通过scanner获取basePackage下的候选类(有标@TableName注解的类)
            Set<BeanDefinition> candidateComponents = scanner.findCandidateComponents(basePackage);
            // 遍历每一个候选类，如果符合条件就把他们注册到容器
            for (BeanDefinition candidateComponent : candidateComponents) {
                if (candidateComponent instanceof AnnotatedBeanDefinition) {
                    // verify annotated class is an interface
                    AnnotatedBeanDefinition beanDefinition = (AnnotatedBeanDefinition) candidateComponent;
                    AnnotationMetadata annotationMetadata = beanDefinition.getMetadata();
                    if (annotationMetadata.isInterface()) {
                        log.warn("The {} is an interface, @TableName can only be specified on an general class", annotationMetadata.getClassName());
                        continue;
                    }

                    // 获取@TableName注解的属性
                    Map<String, Object> attributes = annotationMetadata.getAnnotationAttributes(TableName.class.getCanonicalName());
                    // 注册到容器
                    parseTableName(registry, annotationMetadata, attributes);
                }
            }
        }
    }

    /**
     * 解析表名
     *
     * @param registry           注册表
     * @param annotationMetadata 元数据注释
     * @param attributes         属性
     */
    private void parseTableName(BeanDefinitionRegistry registry, AnnotationMetadata annotationMetadata, Map<String, Object> attributes) {

        // 类名（接口全限定名）
        String className = annotationMetadata.getClassName();

        ObjectMappedStatement objectMappedStatement = new ObjectMappedStatement();
        objectMappedStatement.setId(className);
        objectMappedStatement.setTableName(getTableName(attributes));
        objectMappedStatement.setColumnFamily(getColumnFamily(attributes));
        Map<String, ObjectProperty> properties = parseProperties(className);
        ObjectProperty objectProperty = properties.values().stream().filter(a -> a.getField().isAnnotationPresent(TableId.class)).findAny().orElse(null);
        if (ObjectUtil.isEmpty(objectProperty)) {
            objectProperty = properties.get(QueryConstant.ID);
        }
        Assert.notNull(objectProperty, ExceptionEnum.getException(ExceptionEnum.ID_NOT_FOUND, className));
        objectMappedStatement.setProperties(properties);
        objectMappedStatement.setTableId(objectProperty.getField());
        HbaseMappedFactory.addStatement(className, objectMappedStatement);
    }

    /**
     * 解析属性
     *
     * @param className 类名
     * @return {@link Map}<{@link String}, {@link ObjectProperty}>
     */
    private Map<String, ObjectProperty> parseProperties(String className) {
        Class<Object> clazz = ClassUtil.loadClass(className);
        if (ObjectUtil.isNotNull(clazz)) {
            List<ObjectProperty> objectProperties = CollectionUtil.newArrayList(ReflectUtil.getFields(clazz))
                    .stream().map(a -> {
                        ObjectProperty objectProperty = new ObjectProperty();
                        objectProperty.setField(a);

                        String column = a.getName();
                        if (a.isAnnotationPresent(ColumnName.class)) {
                            column = a.getAnnotation(ColumnName.class).value();
                        }
                        objectProperty.setColumnBytes(Bytes.toBytes(column));
                        objectProperty.setColumn(column);
                        objectProperty.setType(a.getType());
                        return objectProperty;
                    }).collect(Collectors.toList());

           return objectProperties.stream().collect(Collectors.toMap(ObjectProperty::getColumn, a -> a, (a, b) -> a));
        }
        return Collections.emptyMap();
    }

    /**
     * 创建扫描器
     *
     * @return {@link ClassPathScanningCandidateComponentProvider}
     */
    private ClassPathScanningCandidateComponentProvider getScanner() {
        return new ClassPathScanningCandidateComponentProvider(false, environment) {
            @Override
            protected boolean isCandidateComponent(AnnotatedBeanDefinition beanDefinition) {
                boolean isCandidate = false;
                if (beanDefinition.getMetadata().isIndependent()) {
                    if (!beanDefinition.getMetadata().isAnnotation()) {
                        isCandidate = true;
                    }
                }
                return isCandidate;
            }
        };
    }

    /**
     * 获取 指定包名
     *
     * @param importingClassMetadata 进口类元数据
     * @return {@link Set}<{@link String}>
     */
    private Set<String> getBasePackages(AnnotationMetadata importingClassMetadata) {
        Map<String, Object> attributes = importingClassMetadata.getAnnotationAttributes(MappedScan.class.getCanonicalName());
        Set<String> basePackages = new HashSet<>();
        assert attributes != null;
        // value 属性是否有配置值，如果有则添加
        for (String pkg : (String[]) attributes.get("value")) {
            if (StringUtils.hasText(pkg)) {
                basePackages.add(pkg);
            }
        }

        // basePackages 属性是否有配置值，如果有则添加
        for (String pkg : (String[]) attributes.get("basePackages")) {
            if (StringUtils.hasText(pkg)) {
                basePackages.add(pkg);
            }
        }

        // 如果上面两步都没有获取到basePackages，那么这里就默认使用当前项目启动类所在的包为basePackages
        if (basePackages.isEmpty()) {
            basePackages.add(ClassUtils.getPackageName(importingClassMetadata.getClassName()));
        }

        return basePackages;
    }

    /**
     * 获取表名
     *
     * @param attributes 属性
     * @return {@link String}
     */
    private String getTableName(Map<String, Object> attributes) {
        String name = Convert.toStr(attributes.get("name"));
        if (StrUtil.isBlank(name)) {
            name = Convert.toStr(attributes.get("value"));
        }
        return name;
    }

    /**
     * 获取列族名
     *
     * @param attributes 属性
     * @return {@link String}
     */
    private String getColumnFamily(Map<String, Object> attributes) {
        return Convert.toStr(attributes.get("columnFamily"));
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

}
