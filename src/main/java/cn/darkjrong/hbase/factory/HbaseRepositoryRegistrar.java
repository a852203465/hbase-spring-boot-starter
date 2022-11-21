package cn.darkjrong.hbase.factory;

import cn.darkjrong.hbase.repository.HbaseRepository;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ClassUtil;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;

import java.util.List;
import java.util.Set;

/**
 * hbase 持久层接口注册器
 *
 * @author Rong.Jia
 * @date 2022/11/21
 */
@Component
public class HbaseRepositoryRegistrar implements BeanDefinitionRegistryPostProcessor, BeanFactoryAware {

    private BeanFactory beanFactory;

    /**
     * 利用factoryBean创建代理对象，并注册到容器
     */
    private static void registerBeanDefinition(BeanDefinitionRegistry registry, AnnotationMetadata annotationMetadata) {
        String className = annotationMetadata.getClassName();

        BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(className);
        GenericBeanDefinition definition = (GenericBeanDefinition) builder.getRawBeanDefinition();
        definition.getPropertyValues().add("interfaceClass", definition.getBeanClassName());
        definition.setBeanClass(HbaseRepositoryFactoryBean.class);
        definition.setAutowireMode(GenericBeanDefinition.AUTOWIRE_BY_TYPE);
        registry.registerBeanDefinition(className, definition);

//        AbstractBeanDefinition beanDefinition = definition.getBeanDefinition();
//        BeanDefinitionHolder holder = new BeanDefinitionHolder(beanDefinition, className);
//        BeanDefinitionReaderUtils.registerBeanDefinition(holder, registry);
    }

    protected static Set<String> getBasePackages(AnnotationMetadata metadata) {
        return CollectionUtil.newHashSet(ClassUtils.getPackageName(metadata.getClassName()));
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
        List<String> basePackages = AutoConfigurationPackages.get(beanFactory);
        for (String basePackage : basePackages) {
            Set<Class<?>> classes = ClassUtil.scanPackageBySuper(basePackage, HbaseRepository.class);
            for (Class<?> aClass : classes) {
                BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(aClass);
                GenericBeanDefinition definition = (GenericBeanDefinition) builder.getRawBeanDefinition();
                definition.getPropertyValues().add("interfaceClass", definition.getBeanClassName());
                definition.setBeanClass(HbaseRepositoryFactoryBean.class);
                definition.setAutowireMode(GenericBeanDefinition.AUTOWIRE_BY_TYPE);
                registry.registerBeanDefinition(aClass.getName(), definition);
            }
        }
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory configurableListableBeanFactory) throws BeansException {

    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }
}