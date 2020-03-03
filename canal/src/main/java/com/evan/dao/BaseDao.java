package com.evan.dao;

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

    public List<String> getListByTableName(String databaseName, String tableName) {
        try {
            Statement statement = druidDataSource.getConnection().createStatement();
            String sql = "select * from " + databaseName + "." + tableName;
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
            return list;
        } catch (Exception e) {
            log.error(e.getMessage());
            return Lists.newArrayList();
        }
    }

    public String loadToTable(String filepath, String databaseName, String tableName) {
        String sql = "load data local inpath '" + filepath + "' into table " + databaseName + "." + tableName;
        String result = "Load data into table successfully...";

        try {
            hiveDruidTemplate.execute(sql);
        } catch (DataAccessException dae) {
            result = "Load data into table encounter an error: " + dae.getMessage();
            log.error(result);
        }
        return result;
    }
}
