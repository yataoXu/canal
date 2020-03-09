package com.evan.listener;

import cn.hutool.core.date.DateUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.evan.DTO.ResultDTO;
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

import com.evan.service.HiveBuilderSqlService;
import com.evan.util.FileUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

@Slf4j
@CanalEventListener
@RequiredArgsConstructor(onConstructor_ = @Autowired)
public class MyAnnoEventListener {

    private final ConfigParams configParams;

    private final HiveBuilderSqlService hiveBuilderSqlService;

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
        List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
        for (CanalEntry.RowData rowData : rowDataList) {
            StringBuffer values = getBeforeColumnValue(rowData);
            values.append("\n");
            String deleteDir = configParams.getDeletedDirMerge();
            writeData(deleteDir, values.toString(), canalMsg.getSchemaName(), canalMsg.getTableName(), rowChange.getEventType().toString());
        }
    }


    private StringBuffer getBeforeColumnValue(CanalEntry.RowData rowData) {
        StringBuffer values = new StringBuffer();
        for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
            values.append(column.getValue() + "\t");
        }
        values.deleteCharAt(values.length() - 1);
        return values;
    }

    private StringBuffer getAfterColumnValue(CanalEntry.RowData rowData) {
        StringBuffer values = new StringBuffer();
        for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
            values.append(column.getValue() + "\t");
        }
        values.deleteCharAt(values.length() - 1);
        return values;
    }

    private void writeData(String path, String content, String schemaName, String tableName, String type) {
        log.info("库名：{},表名：{}，操作类型：{},数据：{}", schemaName, tableName, type, content);
        String today = DateUtil.today();
        FileUtils.writeFile(path, schemaName, tableName, content, today);
    }


    private void getColumnDataOfInsert(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {

        List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();

        for (CanalEntry.RowData rowData : rowDataList) {
            StringBuffer values = getAfterColumnValue(rowData);
            values.append("\n");
            String insertDir = configParams.getInsertDirMerge();
            writeData(insertDir, values.toString(), canalMsg.getSchemaName(), canalMsg.getTableName(), rowChange.getEventType().toString());
        }
    }

    private void getColumnDataOfUpdate(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {

        List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();

        for (CanalEntry.RowData rowData : rowDataList) {
            StringBuffer values = getBeforeColumnValue(rowData);
            values.append(",");

            StringBuffer afterColumnValue = getAfterColumnValue(rowData);
            values.append(afterColumnValue);

            values.append("\n");

            String updateDir = configParams.getUpdateDirMerge();
            writeData(updateDir, values.toString(), canalMsg.getSchemaName(), canalMsg.getTableName(), rowChange.getEventType().toString());
        }
    }


    @CreateTableListenPoint
    public void onEventCreateTable(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {
        log.info("创建表操作");
        log.info("use " + rowChange.getDdlSchemaName() + ";\n" + rowChange.getSql());
    }

    @DropTableListenPoint
    public void onEventDropTable(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {
        log.info("删除表操作");
        log.info("use " + rowChange.getDdlSchemaName() + ";\n" + rowChange.getSql());

        ResultDTO resultDTO = hiveBuilderSqlService.dropTable(canalMsg.getSchemaName(), canalMsg.getTableName());

        if (resultDTO.getStatus()){
            log.info("删除表{}.{}成功",canalMsg.getSchemaName(),canalMsg.getTableName());
        }else {
            log.error("删除表{}.{}失败",canalMsg.getSchemaName(),canalMsg.getTableName());
        }

    }

//    @AlertTableListenPoint
//    public void onEventAlertTable(CanalEntry.RowChange rowChange ) {
//        log.info("修改表信息操作");
//        log.info("use " + rowChange.getDdlSchemaName() + ";\n" + rowChange.getSql());
//    }
//
//    @CreateIndexListenPoint
//    public void onEventCreateIndex(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
//        log.info("创建索引操作");
//        log.info("use " + canalMsg.getSchemaName() + ";\n" + rowChange.getSql());
//
//    }


}