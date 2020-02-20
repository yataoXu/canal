package com.evan.service;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
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


    //insert overwrite table haha partition(dt='20200220')
    //select  * from (
    //SELECT id,name FROM student.haha where h.dt ='20200218'
    //UNION
    //select id,name from haha_insert where dt = '20200219'
    //)as aa;
    public String createInsertSql(CanalMsg canalMsg, String columnName) {
        DateTime yesterday = DateUtil.yesterday();
        DateTime beforeYesterday = DateUtil.offsetDay(yesterday, -1);
        StringBuffer sql = new StringBuffer();
        List<String> collect = Arrays.stream(columnName.split("\t")).map((s) -> s + ",").collect(Collectors.toList());

        sql.append("insert overwrite table ").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append(" partition(dt='")
                .append(yesterday.toString(DatePattern.PURE_DATE_PATTERN)).append("')")
                .append(" select  * from ( select ");
        collect.forEach(sql::append);
        sql = sql.deleteCharAt(sql.length() - 1);
        sql.append(" from ").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append("  where dt='").append(beforeYesterday.toString(DatePattern.PURE_DATE_PATTERN))
                .append("'  UNION SELECT ");
        collect.forEach(sql::append);
        sql = sql.deleteCharAt(sql.length() - 1);
        sql.append(" from ").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName())
                .append("_insert dt='").append(yesterday.toString(DatePattern.PURE_DATE_PATTERN)).append("')as aa;\n");
        System.out.println(sql);
        return sql.toString();
    }

    // insert overwrite table haha partition(dt='20200220') select  * from ( SELECT h.id,h.name FROM student.haha h LEFT JOIN (select id, name
    // from student.haha_update where dt='20200220') hu on h.id = hu.id where hu.id is null and h.dt ='20200218' UNION
    // select id,name from haha_update where dt = '20200220')as aa;
    public String createUpdateSql(CanalMsg canalMsg, Map<String, String> kv, String columnName) {

        DateTime yesterday = DateUtil.yesterday();
        DateTime beforeYesterday = DateUtil.offsetDay(yesterday, -1);
        StringBuffer sql = new StringBuffer();
        List<String> columnList = Arrays.stream(columnName.split("\t")).map((s) -> s + ",").collect(Collectors.toList());
        sql.append("insert overwrite table ").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append(" partition(dt='")
                .append(yesterday.toString(DatePattern.PURE_DATE_PATTERN)).append("')").append(" select * from (SELECT ");
        for (String column : columnList) {
            sql.append("h_").append(column);
        }
        sql = sql.deleteCharAt(sql.length() - 1);
        sql.append(" FROM ").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append(" h ").append("LEFT JOIN ( select ");
        for (String column : columnList) {
            sql.append(column);
        }
        sql = sql.deleteCharAt(sql.length() - 1);

        sql.append(" from ").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append("_update where dt='")
                .append(yesterday.toString(DatePattern.PURE_DATE_PATTERN)).append("') hu on h.").append(kv.get("key"))
                .append(" = hu.").append(kv.get("key")).append(" where hu.").append(kv.get("key")).append(" is null and h.dt ='")
                .append(beforeYesterday.toString(DatePattern.PURE_DATE_PATTERN)).append("' UNION select ");
        for (String column : columnList) {
            sql.append(column);
        }
        sql = sql.deleteCharAt(sql.length() - 1);
        sql.append(" from ").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append("_update").append(" where dt = '")
                .append(yesterday.toString(DatePattern.PURE_DATE_PATTERN)).append("') as aa;\n");
        System.out.println(sql);
        return sql.toString();
    }


    //insert overwrite table haha partition(dt='20200220')
    //SELECT h.id,h.name FROM student.haha h LEFT JOIN (select id, name from student.haha_delete where dt='20200219') hu on h.id = hu.id
    //where hu.id is null and h.dt ='20200218'
    public String createDeleteSql(CanalMsg canalMsg, Map<String, String> kv, String columnName) {
        StringBuffer sql = new StringBuffer();
        DateTime yesterday = DateUtil.yesterday();
        DateTime beforeYesterday = DateUtil.offsetDay(yesterday, -1);
        List<String> columnList = Arrays.stream(columnName.split("\t")).map((s) -> s + ",").collect(Collectors.toList());
        sql.append("insert overwrite table ").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append(" partition(dt='")
                .append(yesterday.toString(DatePattern.PURE_DATE_PATTERN)).append("') select ");
        for (String column : columnList) {
            sql.append("h_").append(column);
        }
        sql = sql.deleteCharAt(sql.length() - 1);
        sql.append("FROM").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append(" h ").append("LEFT JOIN ( ")
                .append("select");
        for (String column : columnList) {
            sql.append(column);
        }
        sql = sql.deleteCharAt(sql.length() - 1);
        sql.append(" from ").append(canalMsg.getSchemaName() + "." + canalMsg.getTableName()).append("_delete where dt='")
                .append(yesterday.toString(DatePattern.PURE_DATE_PATTERN)).append("') hu on h.").append(kv.get("key")).append(" = hu.").append(kv.get("key")).
                append(" where hu.").append(kv.get("key")).append(" is null and h.dt ='").append(beforeYesterday.toString(DatePattern.PURE_DATE_PATTERN)).append("';\n");
        System.out.println(sql);
        return sql.toString();
    }


    //load data local inpath '/root/evan/haha' into table haha partition(dt='20200218');
    public String createLoadSql(String inPath,CanalMsg canalMsg ){
        DateTime yesterday = DateUtil.yesterday();
        StringBuffer sb =  new StringBuffer();
        sb.append("load data local inpath '").append(inPath).append("' into table")
                .append(canalMsg.getSchemaName()+"."+canalMsg.getTableName()).append(" partition(dt='")
                .append(yesterday.toString(DatePattern.PURE_DATE_PATTERN)).append("');");
        return sb.toString();
    }


}
