package com.evan.canal.annotation.ddl;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.evan.canal.annotation.ListenPoint;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;


/**
 * @Description 重命名表
 * @ClassName AlertTableListenPoint
 * @Author Evan
 * @date 2019.10.14 13:26
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ListenPoint(eventType = CanalEntry.EventType.RENAME)
public @interface RenameTableListenPoint {

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
}
