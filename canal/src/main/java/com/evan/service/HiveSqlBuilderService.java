package com.evan.service;


import com.evan.entity.TableData;

public interface HiveSqlBuilderService {

    void createTable();

    void loadData(String pathFile);

    void createSql();

    void insert(TableData tableData);

    void deleteAll();
}