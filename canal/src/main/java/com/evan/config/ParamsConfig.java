package com.evan.config;

import com.evan.config.property.ConfigParams;
import com.evan.config.property.HiveDruidProperty;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * @Classname ParamsConfig
 * @Description
 * @Date 2020/2/28 22:05
 * @Created by Evan
 */

@Slf4j
@Configuration
@EnableConfigurationProperties(ConfigParams.class)
public class ParamsConfig {

    private final ConfigParams configParams;

    public ParamsConfig(ConfigParams configParams) {
        this.configParams = configParams;
    }

    @Bean(name = "configParams")
    public ConfigParams configParams() {
        return new ConfigParams();
    }

}
