package cn.darkjrong.hbase.factory;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;

import java.lang.reflect.Method;

/**
 * 代理工厂
 *
 * @author Rong.Jia
 * @date 2022/11/22
 */
@Setter
@Getter
public class ProxyFactory<T> implements FactoryBean<T>, MethodInterceptor {

    private Object target;
    private Class<?> interfaces;

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
        }
        return method.invoke(target, args);
    }


}
