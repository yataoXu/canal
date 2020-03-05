package com.evan.dao;

import com.evan.DTO.ResultDTO;
import com.evan.config.property.ConfigParams;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @Classname BaseDao
 * @Description
 * @Date 2020/3/2 10:14
 * @Created by Evan
 */

@Slf4j
@Repository
public class BaseDao {
    @Autowired
    @Qualifier("hiveDruidDataSource")
    DataSource druidDataSource;

    @Autowired
    @Qualifier("hiveDruidTemplate")
    private JdbcTemplate hiveDruidTemplate;

    @Autowired
    ConfigParams configParams;

    public LinkedList<String> getListByTableName(String databaseName, String tableName) {
        try {
            Statement statement = druidDataSource.getConnection().createStatement();
            String sql = "select * from " + databaseName + "." + tableName;
            log.info("Running: " + sql);
            ResultSet res = statement.executeQuery(sql);
            LinkedList<String> list = Lists.newLinkedList();
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
            return list;
        } catch (Exception e) {
            log.error(e.getMessage());
            return Lists.newLinkedList();
        }
    }

    public ResultDTO loadToTable(String filepath, String databaseName, String tableName) {
        ResultDTO result = new ResultDTO();
        String sql = "load data local inpath '" + filepath + "'OVERWRITE into table " + databaseName + "." + tableName;
        log.info("表：{}，执行 sql:{}", tableName, sql);
        try {
            hiveDruidTemplate.execute(sql);
            result.setStatus(true);
        } catch (DataAccessException date) {
            result.setStatus(false);
            result.setMessage("Load data into table encounter an error: " + date.getMessage());
            log.error(date.getMessage());
        }
        return result;
    }

}
