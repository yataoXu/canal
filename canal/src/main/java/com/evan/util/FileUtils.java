package com.evan.util;

import cn.hutool.core.date.DateUtil;
import com.evan.config.property.ConfigParams;
import lombok.extern.slf4j.Slf4j;

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

    public static void writeFile(String path, String SchemaName, String tableName, String content) {

        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            File srcDir = srcDirFolder(SchemaName, tableName, path);
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

    private static File srcDirFolder(String schemaName, String tableName, String path) throws IOException {

        String now = DateUtil.today();

        File srcDirFile = new File(path + "/" + schemaName + "/" + "/" + tableName + "/" + tableName + now);
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
