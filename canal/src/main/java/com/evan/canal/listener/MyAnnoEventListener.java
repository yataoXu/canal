package com.evan.canal.listener;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.evan.canal.annotation.CanalEventListener;
import com.evan.canal.annotation.ddl.AlertTableListenPoint;
import com.evan.canal.annotation.ddl.CreateIndexListenPoint;
import com.evan.canal.annotation.ddl.CreateTableListenPoint;
import com.evan.canal.annotation.ddl.DropTableListenPoint;
import com.evan.canal.annotation.dml.DeleteListenPoint;
import com.evan.canal.annotation.dml.InsertListenPoint;
import com.evan.canal.annotation.dml.UpdateListenPoint;
import com.evan.canal.core.CanalMsg;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * @Description 注解方法
 * @ClassName MyAnnoEventListener
 * @Author Evan
 * @date 2019.10.17 11:28
 */
@CanalEventListener
public class MyAnnoEventListener {

    @InsertListenPoint
    public void onEventInsertData(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        System.out.println("==注解方式（新增数据操作）=======");
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
        for (CanalEntry.RowData rowData : rowDatasList) {
            String sql = "use " + canalMsg.getSchemaName() + ";\n";
            StringBuffer colums = new StringBuffer();
            StringBuffer values = new StringBuffer();
            rowData.getAfterColumnsList().forEach((c) -> {
                colums.append(c.getName() + ",");
                values.append(c.getValue() + "\t");
            });


//            sql += "INSERT INTO " + canalMsg.getTableName() + "(" + colums.substring(0, colums.length() - 1) + ") VALUES(" + values.substring(0, values.length() - 1) + ");";
//            System.out.println(sql);
            System.out.println(values.substring(0, values.length() - 1));
        }
        System.out.println("====================");

    }

    @UpdateListenPoint
    public void onEventUpdateData(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        System.out.println("=====注解方式（更新数据操作）=========");
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
        for (CanalEntry.RowData rowData : rowDatasList) {

            String sql = "use " + canalMsg.getSchemaName() + ";\n";
            StringBuffer updates = new StringBuffer();
            StringBuffer conditions = new StringBuffer();
            rowData.getAfterColumnsList().forEach((c) -> {
                if (c.getIsKey()) {
                    conditions.append(c.getName() + "='" + c.getValue() + "'");
                } else {
                    updates.append(c.getName() + "='" + c.getValue() + "',");
                }
            });
            sql += "UPDATE " + canalMsg.getTableName() + " SET " + updates.substring(0, updates.length() - 1) + " WHERE " + conditions;
            System.out.println(sql);
        }
        System.out.println("\n==========");
    }

    @DeleteListenPoint
    public void onEventDeleteData(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {

        System.out.println("====注解方式（删除数据操作）========");
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
        for (CanalEntry.RowData rowData : rowDatasList) {

            if (!CollectionUtils.isEmpty(rowData.getBeforeColumnsList())) {
                String sql = "use " + canalMsg.getSchemaName() + ";\n";

                sql += "DELETE FROM " + canalMsg.getTableName() + " WHERE ";
                StringBuffer idKey = new StringBuffer();
                StringBuffer idValue = new StringBuffer();
                for (CanalEntry.Column c : rowData.getBeforeColumnsList()) {
                    if (c.getIsKey()) {
                        idKey.append(c.getName());
                        idValue.append(c.getValue());
                        break;
                    }


                }

                sql += idKey + " =" + idValue + ";";
                System.out.println(sql);
            }
            System.out.println("\n===============");

        }
    }

    @CreateTableListenPoint
    public void onEventCreateTable(CanalEntry.RowChange rowChange) {
        System.out.println("======================注解方式（创建表操作）==========================");
        System.out.println("use " + rowChange.getDdlSchemaName() + ";\n" + rowChange.getSql());
        System.out.println("\n======================================================");
    }

    @DropTableListenPoint
    public void onEventDropTable(CanalEntry.RowChange rowChange) {
        System.out.println("======================注解方式（删除表操作）==========================");
        System.out.println("use " + rowChange.getDdlSchemaName() + ";\n" + rowChange.getSql());
        System.out.println("\n======================================================");
    }

    @AlertTableListenPoint
    public void onEventAlertTable(CanalEntry.RowChange rowChange) {
        System.out.println("======================注解方式（修改表信息操作）==========================");
        System.out.println("use " + rowChange.getDdlSchemaName() + ";\n" + rowChange.getSql());
        System.out.println("\n======================================================");
    }

    @CreateIndexListenPoint
    public void onEventCreateIndex(CanalMsg canalMsg,CanalEntry.RowChange rowChange){
        System.out.println("======================注解方式（创建索引操作）==========================");
        System.out.println("use " + canalMsg.getSchemaName()+ ";\n" + rowChange.getSql());
        System.out.println("\n======================================================");

    }


}
