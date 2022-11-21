package cn.darkjrong.hbase.factory;

import cn.darkjrong.hbase.repository.HbaseRepository;
import org.springframework.beans.factory.FactoryBean;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * hbase 持久层 工厂Bean
 *
 * @author Rong.Jia
 * @date 2022/11/21
 */
public class HbaseRepositoryFactoryBean<T> implements FactoryBean<T>, InvocationHandler {

    private Class<T> interfaceClass;

    public Class<T> getInterfaceClass() {
        return interfaceClass;
    }

    public void setInterfaceClass(Class<T> interfaceClass) {
        this.interfaceClass = interfaceClass;
    }

    @Override
    public T getObject() throws Exception {
        final Class<?>[] interfaces = {interfaceClass, HbaseRepository.class};
        return (T) Proxy.newProxyInstance(this.getClass().getClassLoader(), interfaces, this);
    }

    @Override
    public Class<?> getObjectType() {
        return interfaceClass;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // Object 方法，走原生方法,比如hashCode()
        if (Object.class.equals(method.getDeclaringClass())) {
            return method.invoke(this, args);
        }
        // 其它走本地代理
        return method.invoke(this, args);
    }

}