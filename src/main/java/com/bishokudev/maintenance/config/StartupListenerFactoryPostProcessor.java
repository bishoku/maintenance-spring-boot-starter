package com.bishokudev.maintenance.config;

import org.aopalliance.intercept.MethodInterceptor;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.BeanCreationException;

import java.lang.reflect.Modifier;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Defers listener startup after the factory has applied endpoint settings and user customizers,
 * but before the registry can start the returned container. Registry-managed containers are
 * not generally Spring beans, so a container BeanPostProcessor alone cannot intercept them.
 */
final class StartupListenerFactoryPostProcessor implements BeanPostProcessor {
    private final Class<?> factoryType;
    private final BooleanSupplier enabled;
    private final Consumer<Object> defer;

    StartupListenerFactoryPostProcessor(Class<?> factoryType, BooleanSupplier enabled, Consumer<Object> defer) {
        this.factoryType = factoryType;
        this.enabled = enabled;
        this.defer = defer;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        if (!factoryType.isInstance(bean) || !enabled.getAsBoolean()) {
            return bean;
        }
        if (Modifier.isFinal(bean.getClass().getModifiers())) {
            throw new BeanCreationException(beanName, "Startup maintenance requires a non-final listener container factory");
        }
        for (var method : bean.getClass().getMethods()) {
            if ("createListenerContainer".equals(method.getName()) && Modifier.isFinal(method.getModifiers())) {
                throw new BeanCreationException(beanName, "Startup maintenance requires a non-final createListenerContainer method");
            }
        }
        // Preserve concrete factory injection and any existing customizers/proxy advice.
        ProxyFactory proxy = new ProxyFactory(bean);
        proxy.setProxyTargetClass(true);
        proxy.addAdvice((MethodInterceptor) invocation -> {
            Object result = invocation.proceed();
            if ("createListenerContainer".equals(invocation.getMethod().getName())
                    && invocation.getArguments().length == 1 && enabled.getAsBoolean()) {
                defer.accept(result);
            }
            return result;
        });
        return proxy.getProxy(bean.getClass().getClassLoader());
    }
}
