package com.evan.canal.config;

import com.evan.canal.canalClient.SimpleCanalClient;
import com.evan.canal.property.CanalProperties;
import com.evan.canal.util.BeanUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;



/**
 * @Description
 * @ClassName TestConfig
 * @Author Evan
 * @date 2019.10.13 20:32
 */

@Slf4j
@Configuration
@ComponentScan("com.evan.canal")
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
