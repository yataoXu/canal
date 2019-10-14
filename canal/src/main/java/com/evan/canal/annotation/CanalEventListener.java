package com.evan.canal.annotation;

import org.springframework.core.annotation.AliasFor;
import org.springframework.stereotype.Component;

import java.lang.annotation.*;

/**
 * @Description canal 监听器注解，继承 @Component
 * @ClassName CanalEventListener
 * @Author Evan
 * @date 2019.10.14 13:26
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface CanalEventListener {
    @AliasFor(annotation = Component.class)
    String value() default "";
}
