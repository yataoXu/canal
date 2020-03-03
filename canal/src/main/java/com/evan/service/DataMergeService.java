package com.evan.service;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @Classname DataMergeService
 * @Description
 * @Date 2020/2/29 22:33
 * @Created by Evan
 */
public interface DataMergeService {

   void dataMergeDelete(String databaseName) throws IOException, SQLException;

   void dataMergeUpdate(String databaseName) throws IOException, SQLException;

   void dataMergeInsert(String databaseName) throws IOException, SQLException;

}
