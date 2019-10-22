package com.evan.canal.transfer;

/**
 * @Description
 * @ClassName CreateMergeSql
 * @Author Evan
 * @date 2019.10.22 09:09
 */

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.evan.canal.core.CanalMsg;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Date;

/**
 * insert overwrite  table evan.test partition(dt='20191022')
 * select * from (select tt.id,tt.age,tt.name from evan.test tt where tt.dt ='20191022'
 * UNION select t.id,t.age,t.name from evan.test_insert t)as A;
 */

@Slf4j
@Component
public class CreateMergeSql {
    public void createInsertEventSql(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        StringBuffer sql = new StringBuffer();
        sql.append("insert overwrite table ").append(canalMsg.getSchemaName()).append(".")
                .append(canalMsg.getTableName()).append(" partition(dt='");
        // 当前的时间
        Date date = DateUtil.date();
        String dateStr = DateUtil.format(date, DatePattern.PURE_DATE_PATTERN);
        // 昨天的时间
        Date yesterday = DateUtil.yesterday();
        String yesterdayStr = DateUtil.format(yesterday, DatePattern.PURE_DATE_PATTERN);

        String ttColumnNames = getColumnName(rowChange, "tt");
        String tColumnNames = getColumnName(rowChange, "t");
        if (StringUtils.isBlank(ttColumnNames)) {
            log.error("未获取到表{}中的字段属性{}", canalMsg.getTableName(), ttColumnNames);
            throw new RuntimeException(canalMsg.getTableName() + "字段获取异常");
        }
        sql.append(dateStr).append("') ").append("select * from(select ").append(ttColumnNames)
                .append(" from " + canalMsg.getSchemaName()).append(".").append(canalMsg.getTableName())
                .append(" tt where tt.dt= '").append(yesterdayStr)
                .append("' UNION select ").append(tColumnNames)
                .append(" from " + canalMsg.getSchemaName()).append(".").append(canalMsg.getTableName() + "_insert")
                .append(" t)as A;").append("\n");
        System.out.println(sql.toString());
    }


    public void createUpdateEventSql(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        StringBuffer sql = new StringBuffer();
        sql.append("insert overwrite table ").append(canalMsg.getSchemaName()).append(".")
                .append(canalMsg.getTableName()).append(" partition(dt='");
        // 当前的时间
        Date date = DateUtil.date();
        String dateStr = DateUtil.format(date, DatePattern.PURE_DATE_PATTERN);


        String aColumnNames = getColumnName(rowChange, "a");
        String ttColumnNames = getColumnName(rowChange, "tt");
        String ttConditions = getConditions(rowChange, "tt");
        String ttuConditions = getConditions(rowChange, "ttu");
        String tColumnNames = getColumnName(rowChange, "t");
        String uColumnNames = getColumnName(rowChange, "u");
        String uConditions = getConditions(rowChange, "u");


        if (StringUtils.isBlank(ttColumnNames)) {
            log.error("未获取到表{}中的字段属性{}", canalMsg.getTableName(), ttColumnNames);
            throw new RuntimeException(canalMsg.getTableName() + "字段获取异常");
        }
        sql.append(dateStr).append("') ").append("select ").append(aColumnNames).append(" from(select ")
                .append(ttColumnNames).append(" from " + canalMsg.getSchemaName()).append(".").append(canalMsg.getTableName())
                .append(" tt full outer join ").append(canalMsg.getSchemaName()).append(".")
                .append(canalMsg.getTableName() + "_update ttu on ")
                .append(ttConditions + " = " + ttuConditions)
                .append(" where tt.dt= '" + dateStr + "' and " + ttuConditions + " is null UNION select ")
                .append(tColumnNames + " from ( select row_number() over (partition by " + uConditions + " order by create_time desc) num ,")
                .append(uColumnNames)
                .append(" from " + canalMsg.getSchemaName()).append(".").append(canalMsg.getTableName())
                .append("_update u ) t where t.num=1 )as a;use ").append("\n");
        System.out.println(sql.toString());
    }


    public void createDeleteEventSql(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        StringBuffer sql = new StringBuffer();
        sql.append("insert overwrite table ").append(canalMsg.getSchemaName()).append(".")
                .append(canalMsg.getTableName()).append(" partition(dt='");
        // 当前的时间
        Date date = DateUtil.date();
        String dateStr = DateUtil.format(date, DatePattern.PURE_DATE_PATTERN);

        String ttColumnNames = getDeleteColumnName(rowChange, "tt");
        String ttConditions = getDeleteEventConditions(rowChange, "tt");
        String ttdConditions = getDeleteEventConditions(rowChange, "ttd");
        if (StringUtils.isBlank(ttColumnNames)) {
            log.error("未获取到表{}中的字段属性{}", canalMsg.getTableName(), ttColumnNames);
            throw new RuntimeException(canalMsg.getTableName() + "字段获取异常");
        }
        sql.append(dateStr).append("') select ")
                .append(ttColumnNames)
                .append(" from " + canalMsg.getSchemaName()).append(".").append(canalMsg.getTableName())
                .append(" tt full outer join " + canalMsg.getSchemaName()).append(".").append(canalMsg.getTableName())
                .append("_delete ttd on ")
                .append(ttConditions + " = " + ttdConditions)
                .append(" where tt.dt= '" + dateStr + "' and " + ttdConditions + " is null ;").append("\n");
        System.out.println(sql.toString());
    }


    public String getConditions(CanalEntry.RowChange rowChange, String parStr) {
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            StringBuffer conditions = new StringBuffer();
            rowData.getAfterColumnsList().forEach((c) -> {
                if (c.getIsKey()) {
                    conditions.append(parStr).append(".").append(c.getName());
                }
            });
            return conditions.toString();
        }
        return null;
    }

    public String getDeleteEventConditions(CanalEntry.RowChange rowChange, String parStr) {
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            if (!CollectionUtils.isEmpty(rowData.getBeforeColumnsList())) {
                StringBuffer conditions = new StringBuffer();
                for (CanalEntry.Column c : rowData.getBeforeColumnsList()) {
                    if (c.getIsKey()) {
                        conditions.append(parStr).append(".").append(c.getName());
                        break;
                    }
                }
                return conditions.toString();
            }
        }
        return null;
    }

    public String getDeleteColumnName(CanalEntry.RowChange rowChange, String parStr) {
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            StringBuffer values = new StringBuffer();
            rowData.getBeforeColumnsList().forEach((c) -> {
                values.append(parStr).append(".").append(c.getName()).append(",");
            });
            return values.substring(0, values.length() - 1);
        }
        return null;
    }


    public String getColumnName(CanalEntry.RowChange rowChange, String parStr) {
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            StringBuffer values = new StringBuffer();
            rowData.getAfterColumnsList().forEach((c) -> {
                values.append(parStr).append(".").append(c.getName()).append(",");
            });
            return values.substring(0, values.length() - 1);
        }
        return null;
    }
}