package com.evan.util;

import cn.hutool.core.date.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


/**
 * @Classname FileUtils
 * @Description
 * @Date 2020/1/9 16:01
 * @Created by Evan
 */
@Slf4j
public class FileUtils {

//    @Value("${access_log_dir}")
    private static String ACCESS_LOG_DIR = "F:/hadoop/mysql/";
//    @Value("${bakup_log_dir}")
//    private String TO_UPLOAD_LOG_DIR;
//    @Value("${to_upload_log_dir}")
//    private String BAKUP_LOG_DIR;


    public static void writeFile(String eventType, String SchemaName, String tableName, String content) {

        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            File srcDir = srcDirFolder(eventType, SchemaName, tableName);
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

    private static File srcDirFolder(String eventType, String schemaName, String tableName) throws IOException {

        String now = DateUtil.today();

        File srcDirFile = new File(ACCESS_LOG_DIR + "/" + schemaName + "/" + "/" + tableName + "/" + tableName + "_" + now);
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
