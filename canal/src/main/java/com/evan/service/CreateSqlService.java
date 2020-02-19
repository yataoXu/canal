package com.evan.service;

import com.evan.core.CanalMsg;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Classname CreateSqlService
 * @Description
 * @Date 2020/2/19 16:08
 * @Created by Evan
 */
@Slf4j
@Service
public class CreateSqlService {


    public String createInsertSql(CanalMsg canalMsg, String values){
        StringBuffer sql = new StringBuffer();
        List<String> collect = Arrays.stream(values.split("\t")).map((s) -> s+",").collect(Collectors.toList());
        sql.append("insert into ").append(canalMsg.getTableName()).append(" values(");
        collect.forEach(sql::append);
        sql = sql.deleteCharAt(sql.length() - 1);
        sql.append(");\n");
        System.out.println(sql);
        return sql.toString();
    }

    //insert overwrite  table ods_haha select  * from (SELECT h.id,h.name FROM student.ods_haha h LEFT JOIN student.haha_update hu on h.id = hu.id where hu.id is null  UNION select id,name from haha_update) as aa;
    public String createUpdateSql(CanalMsg canalMsg, Map<String,String> kv, String columnName){
        StringBuffer sql = new StringBuffer();
        List<String> columnList = Arrays.stream(columnName.split("\t")).map((s) -> s+",").collect(Collectors.toList());

        sql.append("insert overwrite ").append(canalMsg.getSchemaName()+"."+canalMsg.getTableName()).append(" select * from (SELECT ");
        for(String column :columnList){
            sql.append("h_").append(column);
        }
        sql = sql.deleteCharAt(sql.length() - 1);
        sql.append(" FROM ").append(canalMsg.getSchemaName()+"."+canalMsg.getTableName()).append(" h ").append("LEFT JOIN ")
                .append(canalMsg.getSchemaName()+"."+canalMsg.getTableName()).append("_update hu").append(" on h.").append(kv.get("key"))
                //h.id = hu.id where hu.id is null
        .append(" = hu.").append(kv.get("key")).append(" where hu.").append(kv.get("key")).append(" is null").append(" UNION select ");
                //  id,name from haha_update) as aa;
        for(String column :columnList){
            sql.append(column);
        }
        sql = sql.deleteCharAt(sql.length() - 1);
        sql.append(" from ").append(canalMsg.getSchemaName()+"."+canalMsg.getTableName()).append("_update").append(") as aa;\n");
        System.out.println(sql);
        return sql.toString();
    }

    //insert overwrite  table ods_haha SELECT h.id,h.name FROM student.ods_haha h LEFT JOIN student.haha_delete hu on h.id = hu.id where hu.id is null;
    public String createDeleteSql(CanalMsg canalMsg, Map<String,String> kv, String columnName) {
        StringBuffer sql = new StringBuffer();
        List<String> columnList = Arrays.stream(columnName.split("\t")).map((s) -> s+",").collect(Collectors.toList());

        sql.append("insert overwrite ").append(canalMsg.getSchemaName()+"."+canalMsg.getTableName()).append(" select  * from  (SELECT ");
        for(String column :columnList){
            sql.append("h_").append(column);
        }
        sql = sql.deleteCharAt(sql.length() - 1);
        sql.append("FROM").append(canalMsg.getSchemaName()+"."+canalMsg.getTableName()).append(" h ").append("LEFT JOIN")
                .append(canalMsg.getSchemaName()+"."+canalMsg.getTableName()).append("_delete hu ").append(" on h.").append(kv.get("key"))
                .append(" = hu.").append(kv.get("key")).append("  where hu.").append(kv.get("key")).append(" is null;\n");
        System.out.println(sql);
        return sql.toString();
    }

//    public static void main(String[] args) {
//        String s = "ss\tdd\tddd\tddsdf\tdsfsdf\tsdfsdf\tsdfsdf\t";
//        CanalMsg canalMsg = new CanalMsg("dsfsdf","sdfsd","ewrwer");
//        Map map = MapUtil.newHashMap();
//        map.put("key","key");
//        map.put("value","value");
//        CreateSqlService createSqlService = new CreateSqlService();
//        createSqlService.createUpdateSql(canalMsg,map,s);
//
//    }

}
