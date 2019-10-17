package com.evan.canal.annotation.ddl;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.evan.canal.annotation.ListenPoint;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;


/**
 * @Description 创建索引文件
 * @ClassName CreateIndexListenPoint
 * @Author Evan
 * @date 2019.10.14 13:26
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ListenPoint(eventType = CanalEntry.EventType.CINDEX)
public @interface CreateIndexListenPoint{
	/**
	 * canal 指令
	 * default for all
	 * @return canal destination

	 */
	@AliasFor(annotation = ListenPoint.class)
	String destination() default "";
	
	/**
	 * 数据库实例
	 *
	 * @return canal destination
	 */
	@AliasFor(annotation = ListenPoint.class)
	String[] schema() default {};
	
	/**
	 * 监听的表
	 * default for all
	 *
	 * @return canal destination
	 */
	@AliasFor(annotation = ListenPoint.class)
	String[] table() default {};
}
