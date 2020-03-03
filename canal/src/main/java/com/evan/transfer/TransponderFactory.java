package com.evan.transfer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.evan.annotation.CanalEventListener;
import com.evan.core.ListenerPoint;
import com.evan.config.property.CanalProperties;

import java.util.List;
import java.util.Map;

/**
 * @Description 信息转换工厂类接口层
 * @ClassName TransponderFactory
 * @Author Evan
 * @date 2019.10.14 13:22
 */
@FunctionalInterface
public interface TransponderFactory {


    /**
     *
     * @param connector canal 连接工具
     * @param config canal 链接信息
     * @param listeners 实现接口的监听器
     * @param annoListeners 注解监听拦截
     * @return
     */
    MessageTransponder newTransponder(CanalConnector connector, Map.Entry<String, CanalProperties.Instance> config, List<CanalEventListener> listeners, List<ListenerPoint> annoListeners);
}
