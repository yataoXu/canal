package com.evan.canal.listener;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
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
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.CollectionUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * @Description 注解方法
 * @ClassName MyAnnoEventListener
 * @Author Evan
 * @date 2019.10.17 11:28
 */
@CanalEventListener
@Slf4j
public class MyAnnoEventListener {

    @Value("${access_log_dir}")
    private final String ACCESS_LOG_DIR = "D:/hadoop/mysql";

    @InsertListenPoint
    public void onEventInsertData(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        log.info("注解方式---新增数据操作");
        InsertOrUpdateDate(rowChange, canalMsg);
    }


    @UpdateListenPoint
    public void onEventUpdateData(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        log.info("注解方式---更新数据操作");
        InsertOrUpdateDate(rowChange, canalMsg);
    }

    @DeleteListenPoint
    public void onEventDeleteData(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {

        log.info("注解方式---删除数据操作");
        DeleteData(rowChange, canalMsg);


    }

    public void DeleteData(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
        for (CanalEntry.RowData rowData : rowDatasList) {
            if (!CollectionUtils.isEmpty(rowData.getBeforeColumnsList())) {
                StringBuffer values = new StringBuffer();
                rowData.getBeforeColumnsList().forEach((c) -> {
                    values.append(c.getValue()).append("\t");
                });
                initWriteFile(values, rowChange, canalMsg);
            }
        }
    }

    public void InsertOrUpdateDate(CanalEntry.RowChange rowChange, CanalMsg canalMsg) {
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
        for (CanalEntry.RowData rowData : rowDatasList) {
            StringBuffer values = new StringBuffer();
            rowData.getAfterColumnsList().forEach((c) -> {
                values.append(c.getValue()).append("\t");
            });
            initWriteFile(values, rowChange, canalMsg);
        }

    }


    private void initWriteFile(StringBuffer values, CanalEntry.RowChange rowChange, CanalMsg canalMsg) {
        Date date = DateUtil.date();
        String dateStr = DateUtil.format(date, DatePattern.NORM_DATETIME_MS_PATTERN);
        String columnValue = values.append(rowChange.getEventType()).append("\t").append(dateStr).append("\n").toString();
        writeFile(canalMsg.getSchemaName(), canalMsg.getTableName() + "_" + rowChange.getEventType().toString().toLowerCase(), columnValue);
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
    public void onEventCreateIndex(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        System.out.println("======================注解方式（创建索引操作）==========================");
        System.out.println("use " + canalMsg.getSchemaName() + ";\n" + rowChange.getSql());
        System.out.println("\n======================================================");

    }


    public void writeFile(String databaseName, String tableName, String content) {

        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            File srcDir = srcDirFolder(databaseName, tableName);
            fw = new FileWriter(srcDir, true);
            bw = new BufferedWriter(fw);
            fw.write(content);
            fw.flush();
            fw.close();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != fw) {
                    fw.close();
                }
                if (null != bw) {
                    bw.close();
                }
                bw = null;
                fw = null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private File srcDirFolder(String databaseName, String tableName) throws IOException {

        String now = DateUtil.today();
        File srcDirFile = new File(ACCESS_LOG_DIR + "/" + databaseName + "/" + tableName + "/" + tableName + now);
        if (!srcDirFile.getParentFile().exists()) {
            boolean mkdirs = srcDirFile.getParentFile().mkdirs();
            if (!mkdirs) {
                log.error("{}父文件夹创建失败", srcDirFile);
                throw new RuntimeException("父文件夹创建失败");
            }
        }
        srcDirFile.createNewFile();
        return srcDirFile;
    }


}
