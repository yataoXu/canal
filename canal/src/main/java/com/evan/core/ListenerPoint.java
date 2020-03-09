package com.evan.core;

import com.evan.annotation.ListenPoint;
import com.google.common.collect.Maps;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * @Description 监听 canal 操作
 * @ClassName ListenerPoint
 * @Author Evan
 * @date 2019.10.14 13:35
 */
public class ListenerPoint {
    /**
     * 目标
     */
    private Object target;

    /**
     * 监听的方法和节点
     */
    private Map<Method, ListenPoint> invokeMap = Maps.newHashMap();

    /**
     * 构造方法，设置目标，方法以及注解类型
     *
     * @param target 目标
     * @param method 方法
     * @param anno   注解类型
     * @return
     */
    public ListenerPoint(Object target, Method method, ListenPoint anno) {
        this.target = target;
        this.invokeMap.put(method, anno);
    }

    /**
     * 返回目标类
     *
     * @param
     * @return
     */
    public Object getTarget() {
        return target;
    }

    /**
     * 获取监听的操作方法和节点
     *
     * @param
     * @return
     */
    public Map<Method, ListenPoint> getInvokeMap() {
        return invokeMap;
    }
}
