package com.evan.annotation.ddl;


import com.alibaba.otter.canal.protocol.CanalEntry;
import com.evan.annotation.ListenPoint;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

/**
 * @Description 修改字段属性监听器
 * @ClassName AlertTableListenPoint
 * @Author Evan
 * @date 2019.10.14 13:26
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ListenPoint(eventType = CanalEntry.EventType.ALTER)
public @interface AlertTableListenPoint {
    /**
     * canal 指令
     * default for all
     */
    @AliasFor(annotation = ListenPoint.class)
    String destination() default "";

    /**
     * 数据库实例
     */
    @AliasFor(annotation = ListenPoint.class)
    String[] schema() default {};

    /**
     * 监听的表
     * default for all
     */
    @AliasFor(annotation = ListenPoint.class)
    String[] table() default {};
}
