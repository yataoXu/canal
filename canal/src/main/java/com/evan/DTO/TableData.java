package com.evan.DTO;

import lombok.Data;

import java.util.LinkedHashMap;

/**
 * @Classname TableData
 * @Description
 * @Date 2020/2/21 15:32
 * @Created by Evan
 */

@Data
public class TableData {
    private String schemaName;
    private String tableName;
    private LinkedHashMap<String,String> tableValue;
}
