package com.evan.canal.util;

import cn.hutool.core.date.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @Description
 * @ClassName FileUtil
 * @Author Evan
 * @date 2019.10.22 12:25
 */
@Component
@Slf4j
public class FileUtil {
    public void writeFile(String pathName,String databaseName, String tableName, String content) {

        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            File srcDir = srcDirFolder(pathName,databaseName, tableName);
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

    private File srcDirFolder(String pathName,String databaseName,String tableName) throws IOException {

        String now = DateUtil.today();
        File srcDirFile = new File(pathName + "/" + databaseName + "/" + tableName + "/" + tableName + now);
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
