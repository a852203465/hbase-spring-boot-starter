package cn.darkjrong.hbase.factory;

import cn.darkjrong.hbase.repository.support.SimpleHbaseRepository;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.ReflectUtil;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;

import java.io.Serializable;
import java.lang.reflect.Method;

/**
 * 代理工厂
 *
 * @author Rong.Jia
 * @date 2022/11/22
 */
@AllArgsConstructor
public class ProxyFactory<T> implements FactoryBean<T>, MethodInterceptor {

    private final Object target;
    private final Class<?> interfaces;
    private final SimpleHbaseRepository<T, Serializable> repository;

    @Override
    public T getObject() throws Exception {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(target.getClass());
        enhancer.setInterfaces(new Class[]{interfaces});
        enhancer.setCallback(this);
        return (T) enhancer.create();
    }

    @Override
    public Class<?> getObjectType() {
        return interfaces;
    }

    @Override
    public boolean isSingleton() {
        return Boolean.TRUE;
    }

    @Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy methodProxy) throws Throwable {
        if (Object.class.equals(method.getDeclaringClass())) {
            return method.invoke(target, args);
        }else if (ObjectUtil.isNotNull(ReflectUtil.getMethodOfObj(repository, method.getName(), args))) {
            return method.invoke(repository, args);
        }
        return method.invoke(target, args);
    }


}
