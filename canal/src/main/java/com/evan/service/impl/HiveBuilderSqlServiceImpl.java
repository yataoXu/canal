package com.evan.service.impl;

import com.evan.DTO.ResultDTO;
import com.evan.dao.BaseDao;
import com.evan.service.HiveBuilderSqlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Classname HiveBuilderSqlServiceImple
 * @Description
 * @Date 2020/3/6 9:46
 * @Created by Evan
 */
@Service
public class HiveBuilderSqlServiceImpl implements HiveBuilderSqlService {


    @Autowired
    private BaseDao baseDao;

    @Override
    public ResultDTO dropTable(String schemaName, String sql) {

        ResultDTO resultDTO = baseDao.dropTable(schemaName, sql);

        return resultDTO;
    }
}
