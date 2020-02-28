package com.evan.config;

import com.alibaba.druid.pool.DruidDataSource;
import com.evan.config.property.HiveDruidProperty;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * @Classname HiveDruidConfig
 * @Description
 * @Date 2020/2/21 15:22
 * @Created by Evan
 */

@Slf4j
@Configuration
@EnableConfigurationProperties(HiveDruidProperty.class)
public class HiveDruidConfig {

    HiveDruidProperty hiveDruidProperty;

    public HiveDruidConfig(HiveDruidProperty hiveDruidProperty) {
        this.hiveDruidProperty = hiveDruidProperty;
    }


    @Bean(name = "hiveDruidDataSource")
    @Qualifier("hiveDruidDataSource")
    public DataSource dataSource() {
        DruidDataSource datasource = new DruidDataSource();
        datasource.setUrl(hiveDruidProperty.getUrl());
        datasource.setUsername(hiveDruidProperty.getUser());
        datasource.setPassword(hiveDruidProperty.getPassword());
        datasource.setDriverClassName(hiveDruidProperty.getDriverClassName());

        // pool configuration
        datasource.setInitialSize(hiveDruidProperty.getInitialSize());
        datasource.setMinIdle(hiveDruidProperty.getMinIdle());
        datasource.setMaxActive(hiveDruidProperty.getMaxActive());
        datasource.setMaxWait(hiveDruidProperty.getMaxWait());
        datasource.setTimeBetweenEvictionRunsMillis(hiveDruidProperty.getTimeBetweenEvictionRunsMillis());
        datasource.setMinEvictableIdleTimeMillis(hiveDruidProperty.getMinEvictableIdleTimeMillis());
        datasource.setValidationQuery(hiveDruidProperty.getValidationQuery());
        datasource.setTestWhileIdle(hiveDruidProperty.getTestWhileIdle());
        datasource.setTestOnBorrow(hiveDruidProperty.getTestOnBorrow());
        datasource.setTestOnReturn(hiveDruidProperty.getTestOnReturn());
        datasource.setPoolPreparedStatements(hiveDruidProperty.getPoolPreparedStatements());
        datasource.setMaxPoolPreparedStatementPerConnectionSize(hiveDruidProperty.getMaxPoolPreparedStatementPerConnectionSize());
        return datasource;
    }


    @Bean(name = "hiveDruidTemplate")
    public JdbcTemplate hiveDruidTemplate(@Qualifier("hiveDruidDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

}
