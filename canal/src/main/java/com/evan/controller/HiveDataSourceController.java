package com.evan.controller;

import com.evan.config.property.ConfigParams;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


/**
 * 使用 DataSource 操作 Hive
 */
@RestController
@RequestMapping("/hive")
@Slf4j
public class HiveDataSourceController {

//    @Autowired
//    @Qualifier("hiveJdbcDataSource")
//    org.apache.tomcat.jdbc.pool.DataSource jdbcDataSource;

    @Autowired
    @Qualifier("hiveDruidDataSource")
    DataSource druidDataSource;

    @Autowired
    ConfigParams configParams;

    /**
     * 列举当前Hive库中的所有数据表
     */
    @GetMapping(value = "/table/list/{dataBase}")
    public List<String> listAllTables(@PathVariable("dataBase") String dataBase) throws SQLException {
        List<String> list = new ArrayList<String>();
        // Statement statement = jdbcDataSource.getConnection().createStatement();
        Statement statement = druidDataSource.getConnection().createStatement();
        StringBuffer sql = new StringBuffer();
        sql.append("use ").append(dataBase).append(";");
        sql.append("show tables");
        log.info("Running: " + sql);
        ResultSet res = statement.executeQuery(sql.toString());
        while (res.next()) {
            list.add(res.getString(1));
        }
        return list;
    }


    /**
     * 查询Hive库中的某张数据表字段信息
     */
    @RequestMapping("/table/describe")
    public List<String> describeTable(String tableName) throws SQLException {
        List<String> list = new ArrayList<String>();
        // Statement statement = jdbcDataSource.getConnection().createStatement();
        Statement statement = druidDataSource.getConnection().createStatement();
        String sql = "describe " + tableName;
        log.info("Running: " + sql);
        ResultSet res = statement.executeQuery(sql);
        while (res.next()) {
            list.add(res.getString(1));
        }
        return list;
    }


    /**
     * 查询指定tableName表中的数据
     */
    @GetMapping("/table/select/{dataSourceName}/{tableName}")
    public List<String> selectFromTable(@PathVariable("dataSourceName") String dataSourceName, @PathVariable("tableName") String tableName) throws SQLException {

        Statement statement = druidDataSource.getConnection().createStatement();
        String sql = "select * from " + dataSourceName + "." + tableName;
        log.info("Running: " + sql);
        ResultSet res = statement.executeQuery(sql);
        List<String> list = new ArrayList();
        int count = res.getMetaData().getColumnCount();
        String str = null;
        while (res.next()) {
            str = "";
            for (int i = 1; i < count; i++) {
                str += res.getString(i) + "\t";
            }
            str += res.getString(count);
            list.add(str);
        }

        File updateDir = new File(configParams.getUpdateDir() + dataSourceName);
        File deletedDir = new File(configParams.getDeletedDir() + dataSourceName);
        File insertDir = new File(configParams.getInsertDir() + dataSourceName);

        // 删除的数据
        Lists.newArrayList(deletedDir.listFiles()).stream().forEach(f -> {

        });

        // 更新的数据
        Lists.newArrayList(updateDir.listFiles()).stream().forEach(f -> {
        });





        // 增加的数据
        Lists.newArrayList(insertDir.listFiles()).stream().forEach(f -> {
        });

        // load进库


        return list;

    }

}