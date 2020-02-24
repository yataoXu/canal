package com.evan.config;

import com.evan.canalClient.SimpleCanalClient;
import com.evan.property.CanalProperties;
import com.evan.util.BeanUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;



/**
 * @Description
 * @ClassName CanalClientConfig
 * @Author Evan
 * @date 2019.10.13 20:32
 */

@Slf4j
public class CanalClientConfig {

    @Autowired
    private CanalProperties canalProperties;

    /**
     * get工具类
     *
     * @return
     */

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public BeanUtil beanUtil() {
        return new BeanUtil();
    }

    @Bean
    private SimpleCanalClient canalClient() {
        log.info("正在尝试连接 canal 客户端....");
        //连接 canal 客户端
        SimpleCanalClient canalClient = new SimpleCanalClient(canalProperties);
        log.info("正在尝试开启 canal 客户端....");
        //开启 canal 客户端
        canalClient.start();
        log.info("启动 canal 客户端成功....");
        //返回结果
        return canalClient;
    }


}
