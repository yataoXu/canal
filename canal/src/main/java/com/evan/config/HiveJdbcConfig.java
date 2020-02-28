//package com.evan.config;
//
///**
// * @Classname HiveJdbcConfig
// * @Description
// * @Date 2020/2/21 15:27
// * @Created by Evan
// */
//import lombok.Data;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.tomcat.jdbc.pool.DataSource;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Qualifier;
//import org.springframework.boot.context.properties.ConfigurationProperties;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.core.env.Environment;
//import org.springframework.jdbc.core.JdbcTemplate;
//
//
//@Slf4j
//@Configuration
////@ConfigurationProperties(prefix = "hive")
//@Data
//public class HiveJdbcConfig {
//
//    @Autowired
//    private Environment env;
//
//    @Bean(name = "hiveJdbcDataSource")
//    @Qualifier("hiveJdbcDataSource")
//    public DataSource dataSource() {
//        DataSource dataSource = new DataSource();
//        dataSource.setUrl(env.getProperty("hive.url"));
//        dataSource.setDriverClassName(env.getProperty("hive.driver-class-name"));
//        dataSource.setUsername(env.getProperty("hive.user"));
//        dataSource.setPassword(env.getProperty("hive.password"));
//        // logger.debug("Hive DataSource Inject Successfully...");
//        return dataSource;
//    }
//
//    @Bean(name = "hiveJdbcTemplate")
//    public JdbcTemplate hiveJdbcTemplate(@Qualifier("hiveJdbcDataSource") DataSource dataSource) {
//        return new JdbcTemplate(dataSource);
//    }
//
//}