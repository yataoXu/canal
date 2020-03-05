package com.evan.service.impl;

import cn.hutool.core.io.FileUtil;
import com.evan.DTO.ResultDTO;
import com.evan.config.property.ConfigParams;
import com.evan.DTO.TableDetail;
import com.evan.dao.BaseDao;
import com.evan.service.DataMergeService;
import com.evan.util.FileUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
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
public class DataMergeServiceImpl implements DataMergeService {

    @Autowired
    BaseDao baseDao;

    @Autowired
    ConfigParams configParams;

    public void prepareMerge(TableDetail tableDetail) {
        InputStreamReader reader;
        try {
            // 获得上传的文件
            File file = new File(tableDetail.getFile().getAbsolutePath(), tableDetail.getFile().getName());
            reader = new InputStreamReader(new FileInputStream(file), "UTF-8");
            tableDetail.setReader(reader);
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
                prepareMerge(tableDetail);

                while (true) {
                    try {
                        if ((line = tableDetail.getBfreader().readLine()) == null) break;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    remove(tableDetail.getData(), line);
                }

                laterStage(configParams.getDeletedDirMerge(), configParams.getDeletedDirUpload(), databaseName, f.getName(), tableDetail.getData());

            });
        } else {
            log.error("在指定的文件夹:{}下未找到要同步的数据库:{}", path, databaseName);
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
                prepareMerge(tableDetail);

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

            });
        } else {
            log.error("在指定的文件夹:{}下未找到要同步的数据库:{}", path, databaseName);
        }
    }

    public void dataMergeInsert(@Nullable String databaseName) throws IOException, SQLException {

        String path = configParams.getInsertDirMerge();
        log.info("需要merge的路径:{}", path);
        // 获得要上传的文件夹
        File insertDir = new File(path, databaseName);
        if (FileUtil.exist(insertDir) && FileUtil.isNotEmpty(insertDir)) {
            Lists.newArrayList(insertDir.listFiles()).stream().forEach(f -> {
                log.info("需要merge的文件:{}", f.getAbsolutePath());
                List<String> addList = Lists.newArrayList();
                TableDetail tableDetail = new TableDetail();
                tableDetail.setDatabase(databaseName);
                tableDetail.setFile(f);

                prepareMerge(tableDetail);

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
            });
        } else {
            log.error("在指定的文件夹:{}下未找到要同步的数据库:{}", path, databaseName);
        }
    }


    private void laterStage(String mergePathDir, String uploadPathDir, String databaseName, String fileName, List<String> data) {
        // 将merge好的list落地
        String uploadPath = uploadPathDir + "/" + databaseName + "/" + fileName + "/" + fileName;
        log.info("将merge好的list落地到 {}", uploadPath);
        writeDiskOfUploadData(uploadPathDir, databaseName, fileName, data);
        // 清空mergeDir
        String mergePath = mergePathDir + "/" + databaseName + "/" + fileName + "/" + fileName;
        boolean del = FileUtil.del(mergePath);
        log.info("清空mergeDir {},执行结果为{}", mergePath, del);


        // 将merge好的list load 到hive
        ResultDTO resultDTO = baseDao.loadToTable(uploadPath, databaseName, fileName);
        log.info("将表：{} merge好的数据 load 到hive {}库中", fileName, databaseName);
        if (resultDTO.getStatus()) {
            // 删除 uploadDir下已经上传的文件
            boolean del1 = FileUtil.del(uploadPath);
            log.info("删除 uploadDir:{}下已经上传的文件,执行结果为{}", uploadPath, del1);
        }


    }

    public void writeDiskOfUploadData(String path, String databaseName, String tableName, List<String> data) {
        log.info("库名：{},表名：{}，数据：{}", databaseName, tableName, data);
        String allDataString = String.join("\n", data);
        FileUtils.writeFile(path, databaseName, tableName, allDataString);

    }
}
