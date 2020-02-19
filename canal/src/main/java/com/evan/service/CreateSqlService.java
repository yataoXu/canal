package com.evan.service;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.evan.core.CanalMsg;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.unit.DataUnit;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Classname CreateSqlService
 * @Description
 * @Date 2020/2/19 16:08
 * @Created by Evan
 */
//@Slf4j
//@Service
public class CreateSqlService {


    //insert overwrite  table ods_haha select * from haha h where h.dt ='20200218' UNION SELECT * from haha_insert hi  where hi.dt ='20200218'
    public String createInsertSql(CanalMsg canalMsg, String columnName) {
        DateTime yesterday = DateUtil.yesterday();
        DateTime now = DateTime.now();

        StringBuffer sql = new StringBuffer();
        List<String> collect = Arrays.stream(columnName.split("\t")).map((s) -> s + ",").collect(Collectors.toList());

        sql.append("insert overwrite table ").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append(" select ");
        collect.forEach(sql::append);
        sql = sql.deleteCharAt(sql.length() - 1);
        sql.append(" from ").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append(" h where h.dt='").append(yesterday.toString(DatePattern.PURE_DATE_PATTERN))
                .append("'  UNION SELECT ");
        collect.forEach(sql::append);
        sql = sql.deleteCharAt(sql.length() - 1);
        sql.append(" from ").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName())
                .append("_insert hi.dt='").append( now.toString(DatePattern.PURE_DATE_PATTERN)).append("';\n");
        System.out.println(sql);
        return sql.toString();
    }

    //insert overwrite  table ods_haha select  * from (SELECT h.id,h.name FROM student.ods_haha h LEFT JOIN student.haha_update hu on h.id = hu.id where hu.id is null  UNION select id,name from haha_update) as aa;
    public String createUpdateSql(CanalMsg canalMsg, Map<String, String> kv, String columnName) {
        StringBuffer sql = new StringBuffer();
        List<String> columnList = Arrays.stream(columnName.split("\t")).map((s) -> s + ",").collect(Collectors.toList());

        sql.append("insert overwrite table ").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append(" select * from (SELECT ");
        for (String column : columnList) {
            sql.append("h_").append(column);
        }
        sql = sql.deleteCharAt(sql.length() - 1);
        sql.append(" FROM ").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append(" h ").append("LEFT JOIN ")
                .append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append("_update hu").append(" on h.").append(kv.get("key"))
                //h.id = hu.id where hu.id is null
                .append(" = hu.").append(kv.get("key")).append(" where hu.").append(kv.get("key")).append(" is null").append(" UNION select ");
        //  id,name from haha_update) as aa;
        for (String column : columnList) {
            sql.append(column);
        }
        sql = sql.deleteCharAt(sql.length() - 1);
        sql.append(" from ").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append("_update").append(") as aa;\n");
        System.out.println(sql);
        return sql.toString();
    }

    //insert overwrite  table ods_haha SELECT h.id,h.name FROM student.ods_haha h LEFT JOIN student.haha_delete hu on h.id = hu.id where hu.id is null;
    public String createDeleteSql(CanalMsg canalMsg, Map<String, String> kv, String columnName) {
        StringBuffer sql = new StringBuffer();
        List<String> columnList = Arrays.stream(columnName.split("\t")).map((s) -> s + ",").collect(Collectors.toList());

        sql.append("insert overwrite table ").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append(" select  * from  (SELECT ");
        for (String column : columnList) {
            sql.append("h_").append(column);
        }
        sql = sql.deleteCharAt(sql.length() - 1);
        sql.append("FROM").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append(" h ").append("LEFT JOIN")
                .append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append("_delete hu ").append(" on h.").append(kv.get("key"))
                .append(" = hu.").append(kv.get("key")).append(" where hu.").append(kv.get("key")).append(" is null;\n");
        System.out.println(sql);
        return sql.toString();
    }

    public static void main(String[] args) {
//        String s = "ss\tdd\tddd\tddsdf\tdsfsdf\tsdfsdf\tsdfsdf\t";
//        CanalMsg canalMsg = new CanalMsg("dsfsdf","sdfsd","ewrwer");
//        Map map = MapUtil.newHashMap();
//        map.put("key","key");
//        map.put("value","value");
//        CreateSqlService createSqlService = new CreateSqlService();
//        createSqlService.createUpdateSql(canalMsg,map,s);

        DateTime yesterday = DateUtil.yesterday();
        DateTime now = DateTime.now();

        System.out.println(now.toString(DatePattern.PURE_DATE_PATTERN));

        System.out.println(yesterday.toString(DatePattern.PURE_DATE_PATTERN));


    }

}
