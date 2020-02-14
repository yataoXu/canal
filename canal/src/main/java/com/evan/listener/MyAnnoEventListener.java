package com.evan.listener;

import cn.hutool.core.date.DateUtil;
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
import com.evan.util.FileUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

@Slf4j
@CanalEventListener
@RequiredArgsConstructor(onConstructor_ = @Autowired)
public class MyAnnoEventListener {




    @InsertListenPoint
    public void onEventInsertData(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        log.info("新增数据操作");

        initWriteData(rowChange, canalMsg);
    }

    @UpdateListenPoint
    public void onEventUpdateData(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        log.info("更新数据操作");
        initWriteData(rowChange, canalMsg);
    }

    @DeleteListenPoint
    public void onEventDeleteData(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {

        log.info("删除数据操作");
        String eventType = rowChange.getEventType().toString();
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
        for (CanalEntry.RowData rowData : rowDatasList) {
            String schemaName = canalMsg.getSchemaName();
            String tableName = canalMsg.getTableName();
            StringBuffer values = new StringBuffer();
            rowData.getBeforeColumnsList().forEach((c) -> {
                values.append(c.getValue() + "\t");
            });
            writeDatAdd(values, eventType, schemaName, tableName);

        }
    }

    private void writeDatAdd(StringBuffer values, String eventType, String schemaName, String tableName) {
        values.append(eventType).append("\t").append(DateUtil.now()).append("\n");
        String content = values.toString();
        log.info("库名：{},表名：{}，操作类型：{},数据：{}", schemaName, tableName, eventType, content);
        FileUtils.writeFile(eventType, schemaName, tableName, content);
    }


    private void initWriteData(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {
        String eventType = rowChange.getEventType().toString();
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

        for (CanalEntry.RowData rowData : rowDatasList) {
            String schemaName = canalMsg.getSchemaName();
            String tableName = canalMsg.getTableName();
            StringBuffer values = new StringBuffer();
            rowData.getAfterColumnsList().forEach((c) -> {
                values.append(c.getValue() + "\t");
            });
            writeDatAdd(values, eventType, schemaName, tableName);
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