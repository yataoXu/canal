package com.evan.annotation;

/**
 * @Description
 * @ClassName EnableCanalClient
 * @Author Evan
 * @date 2019.10.17 11:12
 */

import com.evan.config.CanalClientConfig;
import com.evan.config.property.CanalProperties;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import({CanalProperties.class, CanalClientConfig.class})
public @interface EnableCanalClient {
}