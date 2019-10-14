package com.evan.canal.canalClient;

import com.alibaba.otter.canal.client.CanalConnector;
import com.evan.canal.annotation.CanalEventListener;
import com.evan.canal.annotation.ListenPoint;
import com.evan.canal.core.ListenerPoint;
import com.evan.canal.property.CanalProperties;
import com.evan.canal.util.BeanUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Description 通过线程池处理
 * @ClassName SimpleCanalClient
 * @Author Evan
 * @date 2019.10.14 13:08
 */
@Slf4j
public class SimpleCanalClient extends AbstractCanalClient {

    /**
     * 声明一个线程池
     */
    private ThreadPoolExecutor executor;

    /**
     * 通过实现接口的监听器
     */
    protected final List<CanalEventListener> listeners = new ArrayList<>();

    /**
     * 通过注解的方式实现的监听器
     */
    private final List<ListenerPoint> annoListeners = new ArrayList<>();



    /**
     * 构造方法，进行一些基本信息初始化
     *
     * @param canalProperties
     * @return
     */
    public SimpleCanalClient(CanalProperties canalProperties) {
        super(canalProperties);
        //这边上可能需要调整，默认核心线程数5个，最大线程数20个，线程两分钟分钟不执行就。。。
        executor = new ThreadPoolExecutor(5, 20,
                120L, TimeUnit.SECONDS,
                new SynchronousQueue<>(), Executors.defaultThreadFactory());
        //初始化监听器
        initListeners();
    }

    /**
     * @param connector
     * @param config
     * @return
     */
    @Override
    protected void process(CanalConnector connector, Map.Entry<String, CanalProperties.Instance> config) {
        executor.submit(factory.newTransponder(connector, config, listeners, annoListeners));
    }

    /**
     * 关闭 canal 客户端
     *
     * @param
     * @return
     */
    @Override
    public void stop() {
        //停止 canal 客户端
        super.stop();

        //线程池关闭
        executor.shutdown();
    }

    /**
     * 初始化监听器
     *
     * @param
     * @return
     */
    private void initListeners() {
        log.info("{}: 监听器正在初始化....", Thread.currentThread().getName());
        //获取监听器
        List<CanalEventListener> list = BeanUtil.getBeansOfType(CanalEventListener.class);
        //若没有任何监听的，我也不知道引入这个 jar 干什么，直接返回吧
        if (list != null) {
            //若存在目标监听，放入 listenerMap
            listeners.addAll(list);
        }

        //若是你喜欢通过注解的方式去监听的话。。
        Map<String, Object> listenerMap = BeanUtil.getBeansWithAnnotation(com.evan.canal.annotation.CanalEventListener.class);
        //也放入 map
        if (listenerMap != null) {
            for (Object target : listenerMap.values()) {
                //方法获取
                Method[] methods = target.getClass().getDeclaredMethods();
                if (methods != null && methods.length > 0) {
                    for (Method method : methods) {
                        //获取监听的节点
                        ListenPoint l = AnnotationUtils.findAnnotation(method, ListenPoint.class);
                        if (l != null) {
                            annoListeners.add(new ListenerPoint(target, method, l));
                        }
                    }
                }
            }
        }
        //初始化监听结束
        log.info("{}: 监听器初始化完成.", Thread.currentThread().getName());
        //整个项目上下文都没发现监听器。。。
        if (log.isWarnEnabled() && listeners.isEmpty() && annoListeners.isEmpty()) {
            log.warn("{}: 该项目中没有任何监听的目标! ", Thread.currentThread().getName());
        }
    }
}
