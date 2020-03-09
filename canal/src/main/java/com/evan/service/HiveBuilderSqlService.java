package com.evan.service;

import com.evan.DTO.ResultDTO;

/**
 * @Classname HiveBuilderSqlService
 * @Description
 * @Date 2020/3/6 9:45
 * @Created by Evan
 */
public interface HiveBuilderSqlService {

    ResultDTO dropTable(String schemaName, String sql);


}
