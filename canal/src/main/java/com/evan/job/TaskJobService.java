//package com.evan.job;
//
//import cn.hutool.core.date.DatePattern;
//import cn.hutool.core.date.DateTime;
//import cn.hutool.core.date.DateUtil;
//import cn.hutool.core.io.FileUtil;
//import com.evan.util.FileUtils;
//import org.springframework.stereotype.Component;
//
//import java.io.File;
//import java.io.IOException;
//import java.sql.Timestamp;
//import java.time.LocalDate;
//import java.time.ZoneOffset;
//import java.util.Arrays;
//import java.util.Date;
//import java.util.List;
//
///**
// * @Classname taskJobService
// * @Description
// * @Date 2020/2/20 15:48
// * @Created by Evan
// */
////@Component
//public class TaskJobService {
//
//
//    public static String ACCESS_LOG_DIR= "";
//
//    public void ddd(){
//        DateTime yesterday = DateUtil.yesterday();
//
//        // File srcDirFile = new File(ACCESS_LOG_DIR + "/" + schemaName  + "/" + tableName + "/" +   now + "/" +tableName);
//
//    }
//
//    //FileUtils.ACCESS_LOG_DIR+"/evan/haha/20200220/haha_delete
//    //FileUtils.ACCESS_LOG_DIR+"/evan/haha/20200220/haha_insert
//    //FileUtils.ACCESS_LOG_DIR+"/evan/haha/20200220/haha_update
//    public static void main(String[] args) throws IOException {
//
//        DateTime yesterday = DateUtil.yesterday();
//        Date beginOfDay = DateUtil.beginOfDay(yesterday);
//
//        System.out.println(yesterday.getTime());
//        String accessLogDir = null;
////        String accessLogDir = FileUtils.ACCESS_LOG_DIR+"evan/haha/"+yesterday.toString(DatePattern.PURE_DATE_PATTERN)+"/haha_delete";
//        File srcDirFile = FileUtil.newFile(accessLogDir);
////        if (!srcDirFile.getParentFile().exists()) {
////            srcDirFile.getParentFile().mkdirs();
////        }
////        srcDirFile.createNewFile();
//
//        System.out.println(srcDirFile.lastModified());
//        System.out.println(System.currentTimeMillis());
//
//        List<File> files = FileUtil.loopFiles(accessLogDir);
//        files.stream().forEach(System.out::println);
//
//        System.out.println(FileUtil.newerThan(srcDirFile, beginOfDay.getTime()));
//
//
//    }
//}
