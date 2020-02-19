package com.evan.listener;

import cn.hutool.core.map.MapUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.evan.annotation.CanalEventListener;
import com.evan.annotation.ddl.AlertTableListenPoint;
import com.evan.annotation.ddl.CreateIndexListenPoint;
import com.evan.annotation.ddl.CreateTableListenPoint;
import com.evan.annotation.ddl.DropTableListenPoint;
import com.evan.annotation.dml.DeleteListenPoint;
import com.evan.annotation.dml.InsertListenPoint;
import com.evan.annotation.dml.UpdateListenPoint;
import com.evan.core.CanalMsg;
import com.evan.service.CreateSqlService;
import com.evan.util.FileUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

@Slf4j
@CanalEventListener
@RequiredArgsConstructor(onConstructor_ = @Autowired)
public class MyAnnoEventListener {

    private CreateSqlService createSqlService;

    @InsertListenPoint
    public void onEventInsertData(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        log.info("新增数据操作");

        getColumnDataOfInsert(rowChange, canalMsg);
    }

    @UpdateListenPoint
    public void onEventUpdateData(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        log.info("更新数据操作");
        getColumnDataInsertOfUpdate(rowChange, canalMsg);
    }

    @DeleteListenPoint
    public void onEventDeleteData(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {

        log.info("删除数据操作");
        String eventType = rowChange.getEventType().toString();
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
        boolean flage = true;
        for (CanalEntry.RowData rowData : rowDatasList) {
            String schemaName = canalMsg.getSchemaName();
            String tableName = canalMsg.getTableName();
            StringBuffer columnName = new StringBuffer();
            StringBuffer values = new StringBuffer();
            Map<String, String> kv = MapUtil.newHashMap();

            for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                boolean isKey = column.getIsKey();
                if (isKey) {
                    kv.put("key", column.getName());
                    kv.put("value", column.getValue());
                }
                columnName.append(column.getName() + "\t");
                values.append(column.getValue() + "\t");
            }

            writeData(values.toString(), eventType, schemaName, tableName);

            if (flage) {
                String deleteSql = createSqlService.createDeleteSql(canalMsg, kv, columnName.toString());
                writeData(deleteSql, eventType + "Sql", schemaName, schemaName);
                flage = false;
            }
        }
    }

    private void writeData(String content, String eventType, String schemaName, String tableName) {
        log.info("库名：{},表名：{}，操作类型：{},数据：{}", schemaName, tableName, eventType, content);
        FileUtils.writeFile(eventType, schemaName, tableName, content);
    }


    private void getColumnDataOfInsert(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {

        String eventType = rowChange.getEventType().toString();
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

        for (CanalEntry.RowData rowData : rowDatasList) {
            String schemaName = canalMsg.getSchemaName();
            String tableName = canalMsg.getTableName();
            StringBuffer values = new StringBuffer();
            rowData.getAfterColumnsList().forEach((c) -> {
                values.append(c.getValue() + "\t");
            });

            String insertSql = createSqlService.createInsertSql(canalMsg, values.toString());
            writeData(insertSql,eventType + "Sql", schemaName, tableName);
            writeData(values.toString(), eventType, schemaName, tableName);
        }
    }

    private void getColumnDataInsertOfUpdate(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {

        String eventType = rowChange.getEventType().toString();
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

        boolean flage = true;
        for (CanalEntry.RowData rowData : rowDatasList) {
            String schemaName = canalMsg.getSchemaName();
            String tableName = canalMsg.getTableName();
            StringBuffer columnName = new StringBuffer();
            StringBuffer values = new StringBuffer();
            Map<String, String> kv = MapUtil.newHashMap();
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                boolean isKey = column.getIsKey();
                if (isKey) {
                    kv.put("key", column.getName());
                    kv.put("value", column.getValue());
                }
                columnName.append(column.getName() + "\t");
                values.append(column.getValue() + "\t");
            }
            if (flage) {
                String updateSql = createSqlService.createUpdateSql(canalMsg, kv, columnName.toString());
                writeData(updateSql, eventType + "Sql", schemaName, schemaName);
                flage = false;
            }
            writeData(values.toString(), eventType, schemaName, tableName);
        }
    }

    @CreateTableListenPoint
    public void onEventCreateTable(CanalEntry.RowChange rowChange) {
        log.info("创建表操作");
        log.info("use " + rowChange.getDdlSchemaName() + ";\n" + rowChange.getSql());
    }

    @DropTableListenPoint
    public void onEventDropTable(CanalEntry.RowChange rowChange) {
        log.info("删除表操作");
        log.info("use " + rowChange.getDdlSchemaName() + ";\n" + rowChange.getSql());
    }

    @AlertTableListenPoint
    public void onEventAlertTable(CanalEntry.RowChange rowChange) {
        log.info("修改表信息操作");
        log.info("use " + rowChange.getDdlSchemaName() + ";\n" + rowChange.getSql());
    }

    @CreateIndexListenPoint
    public void onEventCreateIndex(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        log.info("创建索引操作");
        log.info("use " + canalMsg.getSchemaName() + ";\n" + rowChange.getSql());

    }


}