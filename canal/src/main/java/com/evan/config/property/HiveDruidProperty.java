package com.evan.config.property;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @Classname HiveDruidProperty
 * @Description
 * @Date 2020/2/28 19:21
 * @Created by Evan
 */
@ConfigurationProperties(prefix = "hive")
@Data
public class HiveDruidProperty {
    private String url;
    private String user;
    private String password;
    private String driverClassName;
    private int initialSize;
    private int minIdle;
    private int maxActive;
    private int maxWait;
    private int timeBetweenEvictionRunsMillis;
    private int minEvictableIdleTimeMillis;
    private String validationQuery;
    private Boolean testWhileIdle;
    private Boolean testOnBorrow;
    private Boolean testOnReturn;
    private Boolean poolPreparedStatements;
    private int maxPoolPreparedStatementPerConnectionSize;
}
