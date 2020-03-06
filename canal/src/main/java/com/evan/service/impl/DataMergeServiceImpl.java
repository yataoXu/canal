package com.evan.service.impl;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import com.evan.DTO.ResultDTO;
import com.evan.DTO.TableDetail;
import com.evan.config.property.ConfigParams;
import com.evan.dao.BaseDao;
import com.evan.service.DataMergeService;
import com.evan.util.FileUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTimeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.*;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @Classname DataMergeService
 * @Description
 * @Date 2020/2/29 22:33
 * @Created by Evan
 */
@Slf4j
@Service
@RequiredArgsConstructor(onConstructor_ = @Autowired)
public class DataMergeServiceImpl implements DataMergeService {

    private final BaseDao baseDao;

    private final ConfigParams configParams;

    public boolean prepareMerge(TableDetail tableDetail) {
        InputStreamReader reader;
        try {
            String yesterday = DateUtil.yesterday().toString(DatePattern.NORM_DATE_PATTERN);
            String uploadFileName = tableDetail.getFile().getName() + yesterday;
            // 获得上传的文件
            File file = new File(tableDetail.getFile().getAbsolutePath(), uploadFileName);

            if (FileUtil.exist(file) && FileUtil.isNotEmpty(file)) {
                reader = new InputStreamReader(new FileInputStream(file), "UTF-8");
                tableDetail.setReader(reader);
            } else {
                log.error("数据为空，上传目录为：{}, 文件名为{}", tableDetail.getFile().getAbsolutePath(), uploadFileName);
                return false;
            }

        } catch (UnsupportedEncodingException e1) {
            e1.printStackTrace();
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }

        // 得到原有数据
        try {
            LinkedList<String> list = baseDao.getListByTableName(tableDetail.getDatabase(), tableDetail.getFile().getName());
            tableDetail.setData(list);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        BufferedReader bfreader = new BufferedReader(tableDetail.getReader());
        tableDetail.setBfreader(bfreader);

        return true;
    }


    public void remove(List<String> list, String elem) {
        if (CollectionUtils.isEmpty(list)) return;
        for (int i = list.size() - 1; i >= 0; i--) {
            if (list.get(i).equals(elem)) {
                list.remove(list.get(i));
                break;
            }
        }
    }

    public void dataMergeDelete(String databaseName) throws IOException, SQLException {

        String path = configParams.getDeletedDirMerge();
        // 获得上传的文件夹
        File insertDir = new File(path, databaseName);
        if (FileUtil.exist(insertDir) && FileUtil.isNotEmpty(insertDir)) {
            Lists.newArrayList(insertDir.listFiles()).stream().forEach(f -> {

                String line = null;
                TableDetail tableDetail = new TableDetail();
                tableDetail.setDatabase(databaseName);
                tableDetail.setFile(f);
                if (prepareMerge(tableDetail)) {
                    while (true) {
                        try {
                            if ((line = tableDetail.getBfreader().readLine()) == null) break;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        remove(tableDetail.getData(), line);
                    }

                    laterStage(configParams.getDeletedDirMerge(), configParams.getDeletedDirUpload(), databaseName, f.getName(), tableDetail.getData());
                }
            });
        } else {
            log.info("在指定的文件夹: {}下未找到要同步的数据库: {}", path, databaseName);
        }
    }

    public void dataMergeUpdate(String databaseName) throws IOException, SQLException {
        String path = configParams.getUpdateDirMerge();

        // 获得上传的文件夹
        File insertDir = new File(path, databaseName);
        if (FileUtil.exist(insertDir) && FileUtil.isNotEmpty(insertDir)) {
            Lists.newArrayList(insertDir.listFiles()).stream().forEach(f -> {
                TableDetail tableDetail = new TableDetail();
                tableDetail.setDatabase(databaseName);
                tableDetail.setFile(f);

                if (prepareMerge(tableDetail)) {
                    Map<String, String> map = Maps.newLinkedHashMap();
                    String line = null;
                    while (true) {
                        try {
                            if ((line = tableDetail.getBfreader().readLine()) == null) break;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        String[] split = line.split(",");
                        map.put(split[0], split[1]);
                    }

                    for (Map.Entry<String, String> param : map.entrySet()) {
                        for (int i = 0; i < tableDetail.getData().size(); i++) {
                            if (tableDetail.getData().get(i).equals(param.getKey())) {
                                tableDetail.getData().remove(tableDetail.getData().get(i));
                                tableDetail.getData().add(param.getValue());
                            }
                        }
                    }

                    laterStage(configParams.getUpdateDirMerge(), configParams.getUpdateDirUpload(), databaseName, f.getName(), tableDetail.getData());
                }
            });
        } else {
            log.info("在指定的文件夹: {}下未找到要同步的数据库: {}", path, databaseName);
        }
    }

    public void dataMergeInsert(@Nullable String databaseName) throws IOException, SQLException {

        String path = configParams.getInsertDirMerge();
        log.info("需要merge的路径: {}", path);
        // 获得要上传的文件夹
        File insertDir = new File(path, databaseName);
        if (FileUtil.exist(insertDir) && FileUtil.isNotEmpty(insertDir)) {
            Lists.newArrayList(insertDir.listFiles()).stream().forEach(f -> {
                log.info("需要merge的文件: {}", f.getAbsolutePath());
                List<String> addList = Lists.newArrayList();
                TableDetail tableDetail = new TableDetail();
                tableDetail.setDatabase(databaseName);
                tableDetail.setFile(f);

                if (prepareMerge(tableDetail)) {
                    String line = null;
                    while (true) {
                        try {
                            if ((line = tableDetail.getBfreader().readLine()) == null) break;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        addList.add(line);
                    }
                    if (!addList.isEmpty()) {
                        tableDetail.getData().addAll(addList);
                    }

                    laterStage(configParams.getInsertDirMerge(), configParams.getInsertDirUpload(), databaseName, f.getName(), tableDetail.getData());
                }
            });
        } else {
            log.info("在指定的文件夹:{}下未找到要同步的数据库:{}", path, databaseName);
        }
    }


    private void laterStage(String mergePathDir, String uploadPathDir, String databaseName, String fileName, List<String> data) {

        String yesterday = DateUtil.yesterday().toString(DatePattern.NORM_DATE_PATTERN);
        // 将merge好的list落地
        String mergeFileName = fileName + yesterday;


        writeDiskOfUploadData(uploadPathDir, databaseName, fileName, data);
        log.info("将merge好的list落地到: {}", uploadPathDir);

        // 清空mergeDir下指定文件
        String mergePath = mergePathDir + "/" + databaseName + "/" + fileName + "/" + mergeFileName;
        boolean del = FileUtil.del(mergePath);
        log.info("清空mergeDir: {},执行结果为: {}", mergePath, del);


        String uploadPath = uploadPathDir + "/" + databaseName + "/" + fileName + "/" + mergeFileName;
        // 将merge好的list load 到hive
        ResultDTO resultDTO = baseDao.loadToTable(uploadPath, databaseName, fileName);
        log.info("将表: {} merge好的数据 load 到hive: {}库中", fileName, databaseName);
        if (resultDTO.getStatus()) {
            // 将 uploadDir下已经上传的文件备份到backup目录下
            String backup = configParams.getBackup() + "/" + fileName + DateTimeUtils.currentTimeMillis();
            FileUtil.move(new File(uploadPath), new File(backup), true);
            log.info("将 uploadDir: {}下已经上传的文件,备份到backup目录下，文件名为: {}", uploadPath, backup);
        }


    }

    public void writeDiskOfUploadData(String path, String databaseName, String tableName, List<String> data) {

        log.info("库名：{},表名：{}，数据：{}", databaseName, tableName, data);
        String yesterday = DateUtil.yesterday().toString(DatePattern.NORM_DATE_PATTERN);
        String allDataString = String.join("\n", data);
        FileUtils.writeFile(path, databaseName, tableName, allDataString, yesterday);

    }
}
