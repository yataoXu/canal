package com.evan.service.impl;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import com.evan.config.property.ConfigParams;
import com.evan.core.TableDetail;
import com.evan.dao.BaseDao;
import com.evan.service.DataMergeService;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.sql.SQLException;
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


    public void dataMergeDelete(String databaseName) throws IOException, SQLException {

        String path = configParams.getDeletedDirMerge();
        // 获得上传的文件夹
        File insertDir = new File(path, databaseName);
        Lists.newArrayList(insertDir.listFiles()).stream().forEach(f -> {

            String line = null;
            TableDetail tableDetail = new TableDetail();
            tableDetail.setDatabase(databaseName);
            tableDetail.setFile(f);
            mergeInit(tableDetail);

            while (true) {
                try {
                    if (!((line = tableDetail.getBfreader().readLine()) != null)) break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                remove(tableDetail.getData(), line);
            }

            // 将merge好的list load 到hive
            String filePaht = "";
            baseDao.loadToTable(filePaht, databaseName, f.getName());
        });
    }

    public void mergeInit(TableDetail tableDetail) {
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
            List<String> list = baseDao.getListByTableName(tableDetail.getDatabase(), tableDetail.getFile().getName());
            tableDetail.setData(list);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        BufferedReader bfreader = new BufferedReader(tableDetail.getReader());
        tableDetail.setBfreader(bfreader);
    }


    public void remove(List<String> list, String elem) {
        if (list.isEmpty()) return;
        for (int i = list.size() - 1; i >= 0; i--) {
            if (list.get(i).equals(elem)) {
                list.remove(list.get(i));
                break;
            }
        }
    }


    public void dataMergeUpdate(String databaseName) throws IOException, SQLException {


        String path = configParams.getUpdateDirMerge();

        // 获得上传的文件夹
        File insertDir = new File(path, databaseName);
        Lists.newArrayList(insertDir.listFiles()).stream().forEach(f -> {

            TableDetail tableDetail = new TableDetail();
            tableDetail.setDatabase(databaseName);
            tableDetail.setFile(f);
            mergeInit(tableDetail);


            Map<String, String> map = MapUtil.newHashMap();
            String line = null;
            while (true) {
                try {
                    if (!((line = tableDetail.getBfreader().readLine()) != null)) break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                String[] split = line.split(",");
                map.put(split[0], split[1]);
            }

            for (Map.Entry<String, String> param : map.entrySet()) {
                for (int i = tableDetail.getData().size() - 1; i >= 0; i--) {
                    if (tableDetail.getData().get(i).equals(param.getKey())) {
                        tableDetail.getData().remove(tableDetail.getData().get(i));
                        tableDetail.getData().add(param.getValue());
                    }
                }
            }

            // 将merge好的list load 到hive
            String filePaht = "";
            baseDao.loadToTable(filePaht, databaseName, f.getName());
        });
    }

    public void dataMergeInsert(String databaseName) throws IOException, SQLException {

        String path = configParams.getInsertDirMerge();
        // 获得要上传的文件夹
        File insertDir = new File(path, databaseName);
        Lists.newArrayList(insertDir.listFiles()).stream().forEach(f -> {
            List<String> addList = Lists.newArrayList();
            TableDetail tableDetail = new TableDetail();
            tableDetail.setDatabase(databaseName);
            tableDetail.setFile(f);
            mergeInit(tableDetail);


            String line = null;
            while (true) {
                try {
                    if (!((line = tableDetail.getBfreader().readLine()) != null)) break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                addList.add(line);
            }
            if (!addList.isEmpty()){
                tableDetail.getData().addAll(addList);
            }
            tableDetail.getData().stream().forEach(System.out::println);

            // 将merge好的list落地




            // 将merge好的list load 到hive
            String filePath = configParams.getInsertDirUpload();
            baseDao.loadToTable(filePath, databaseName, f.getName());
        });
    }


}
