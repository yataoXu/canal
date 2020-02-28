package com.evan.service.impl;

import com.evan.entity.TableData;
import com.evan.service.HiveSqlBuilderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.util.List;

/**
 * @Classname HiveBuildServiceImpl
 * @Description
 * @Date 2020/2/21 15:35
 * @Created by Evan
 */
@Service
@Slf4j
public class HiveSqlBuilderServiceImpl implements HiveSqlBuilderService {

    @Autowired
    JdbcTemplate hiveJdbcTemplate;

    @Override
    public void createTable() {

    }

    @Override
    public void loadData(String pathFile) {

    }

    @Override
    public void createSql() {

    }

    @Override
    public void insert(TableData tableData) {

    }

    @Override
    public void deleteAll() {

    }

}
