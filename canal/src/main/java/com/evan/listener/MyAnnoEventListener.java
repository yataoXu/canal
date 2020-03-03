package com.evan.listener;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.evan.annotation.CanalEventListener;
import com.evan.annotation.ddl.AlertTableListenPoint;
import com.evan.annotation.ddl.CreateIndexListenPoint;
import com.evan.annotation.ddl.CreateTableListenPoint;
import com.evan.annotation.ddl.DropTableListenPoint;
import com.evan.annotation.dml.DeleteListenPoint;
import com.evan.annotation.dml.InsertListenPoint;
import com.evan.annotation.dml.UpdateListenPoint;
import com.evan.config.property.ConfigParams;
import com.evan.core.CanalMsg;
import com.evan.service.impl.CreateSqlService;
import com.evan.util.FileUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

@Slf4j
@CanalEventListener
@RequiredArgsConstructor(onConstructor_ = @Autowired)
public class MyAnnoEventListener {

    private final CreateSqlService createSqlService;
    private final ConfigParams configParams;

    @InsertListenPoint
    public void onEventInsertData(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        log.info("新增数据操作");

        getColumnDataOfInsert(rowChange, canalMsg);
    }

    @UpdateListenPoint
    public void onEventUpdateData(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        log.info("更新数据操作");
        getColumnDataOfUpdate(rowChange, canalMsg);
    }

    @DeleteListenPoint
    public void onEventDeleteData(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {

        log.info("删除数据操作");
        String eventType = rowChange.getEventType().toString();
        List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
        for (CanalEntry.RowData rowData : rowDataList) {
            StringBuffer values = new StringBuffer();

            for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                values.append(column.getValue() + "\t");
            }
            String deleteDir = configParams.getDeletedDirMerge();
            writeData(deleteDir, values.toString(), eventType, canalMsg.getSchemaName(), canalMsg.getTableName());
        }
    }

    private void writeData(String path, String content, String eventType, String schemaName, String tableName) {
        log.info("库名：{},表名：{}，操作类型：{},数据：{}", schemaName, tableName, eventType, content);
        FileUtils.writeFile(path, schemaName, tableName, content);
    }


    private void getColumnDataOfInsert(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {

        String eventType = rowChange.getEventType().toString();
        List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();

        for (CanalEntry.RowData rowData : rowDataList) {
            StringBuffer values = new StringBuffer();
            rowData.getAfterColumnsList().forEach((c) -> {
                values.append(c.getValue() + "\t");
            });

            String insertDir = configParams.getInsertDirMerge();
            writeData(insertDir, values.toString(), eventType, canalMsg.getSchemaName(), canalMsg.getTableName());
        }
    }

    private void getColumnDataOfUpdate(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {

        String eventType = rowChange.getEventType().toString();
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

        for (CanalEntry.RowData rowData : rowDatasList) {
            StringBuffer values = new StringBuffer();
            for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                values.append(column.getValue() + "\t");
            }
            values.deleteCharAt(values.length() - 1);
            values.append(",");
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                values.append(column.getValue() + "\t");
            }

            String updateDir = configParams.getUpdateDirMerge();
            writeData(updateDir, values.toString(), eventType, canalMsg.getSchemaName(), canalMsg.getTableName());
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