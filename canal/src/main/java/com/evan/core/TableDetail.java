package com.evan.core;

import lombok.Data;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

/**
 * @Classname TableDetail
 * @Description
 * @Date 2020/3/2 15:44
 * @Created by Evan
 */

@Data
public class TableDetail {

    private String database;

    private String tableName;

    private LinkedList<String> data;

    private String path;

    private File file;

    private InputStreamReader reader;

    private BufferedReader bfreader;
}
