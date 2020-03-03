package com.evan;

import cn.hutool.core.map.MapUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.*;
import java.util.*;

/**
 * @Classname TestFile
 * @Description
 * @Date 2020/2/28 23:16
 * @Created by Evan
 */
public class TestFile {

    @Test
    public void Test01() throws IOException {
        List<String> list1 = new ArrayList();
        list1.add("1	10.2.196.30	3306	root	root	mysql_to_hive	0	2019-09-28 22:23:12	2020-02-24 13:08:12");
        list1.add("2	10.10.1.142	3306	root	anydata@qaz	jira	0	2019-09-28 22:23:12	2019-09-28 22:23:12");
        list1.add("3	10.10.1.64	3306	canal	Canal@123	komdb	0	2019-10-09 23:54:20	2020-02-24 09:26:54");
        list1.add("4	10.10.1.64	3306	canal	Canal@123	eisoo_db	0	2019-10-09 23:54:20	2020-02-24 09:26:57");

        String path = "F:\\hadoop\\mysql\\bakup\\mysql_to_hive\\datasource_config\\datasource_config";
        File deletedDir = new File(path);

        // 删除的数据
        InputStreamReader reader = null;
        try {
            reader = new InputStreamReader(new FileInputStream(deletedDir), "UTF-8");
        } catch (UnsupportedEncodingException e1) {
            e1.printStackTrace();
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        BufferedReader bfreader = new BufferedReader(reader);
        String line;
        while ((line = bfreader.readLine()) != null) {
            remove(list1, line);
        }
        list1.stream().forEach(System.out::print);
    }


    @Test
    public void TestADD() throws IOException {
        List<String> list1 = new ArrayList();
        list1.add("1	10.2.196.30	3306	root	anyrobot123	mysql_to_hive	0	2019-09-28 22:23:12	2020-02-24 13:08:12");
        list1.add("2	10.10.1.142	3306	anydata	anydata@qaz	jira	0	2019-09-28 22:23:12	2019-09-28 22:23:12");
        list1.add("3	10.10.1.64	3306	canal	Canal@123	komdb	0	2019-10-09 23:54:20	2020-02-24 09:26:54");
        list1.add("4	10.10.1.64	3306	canal	Canal@123	eisoo_db	0	2019-10-09 23:54:20	2020-02-24 09:26:57");

        String path = "F:\\hadoop\\mysql\\bakup\\mysql_to_hive\\datasource_config\\datasource_config";
        File deletedDir = new File(path);

        // 删除的数据
        InputStreamReader reader = null;
        try {
            reader = new InputStreamReader(new FileInputStream(deletedDir), "UTF-8");
        } catch (UnsupportedEncodingException e1) {
            e1.printStackTrace();
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }


        List<String> list = new ArrayList<>();
        BufferedReader bfreader = new BufferedReader(reader);
        String line;
        while ((line = bfreader.readLine()) != null) {
            list.add(line);
        }
        add(list1, list);
        list1.stream().forEach(System.out::println);
    }

    @Test
    public void testUpdate() throws IOException {
        List<String> list1 = new ArrayList();
        list1.add("datasource_config\t4");
        list1.add("dim_ddl_convert\t22");

        String path = "F:\\hadoop\\mysql\\bakup\\mysql_to_hive\\mysql_to_hive\\mysql_to_hive_update";
        File updateFile = new File(path);

        InputStreamReader reader = null;
        try {
            reader = new InputStreamReader(new FileInputStream(updateFile), "UTF-8");
        } catch (UnsupportedEncodingException e1) {
            e1.printStackTrace();
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }


        Map<String, String> map = MapUtil.newHashMap();
        BufferedReader bfreader = new BufferedReader(reader);
        String line;
        while ((line = bfreader.readLine()) != null) {
            System.out.println(line);
            String[] split = line.split(",");
            map.put(split[0], split[1]);
        }

        for (Map.Entry<String, String> param : map.entrySet()) {
            for (int i = list1.size() - 1; i >= 0; i--) {
                if (list1.get(i).equals(param.getKey())) {
                    list1.remove(list1.get(i));
                    list1.add(param.getValue());
                }
            }
        }

        list1.stream().forEach(System.out::println);
    }

    public void remove(List<String> list, String elem) {
        for (int i = list.size() - 1; i >= 0; i--) {
            if (list.get(i).equals(elem)) {
                list.remove(list.get(i));
            }
        }
    }

    public void add(List<String> list, List<String> append) {
        list.addAll(append);
    }


    @Test
    public void testadd() {
        String path = "F:/hadoop/mysql/toupload/";
        String database = "mysql_to_hive";

        // 获得要上传的文件夹
        File insertDir = new File(path , database);
        Lists.newArrayList(insertDir.listFiles()).stream().forEach(f -> {
            InputStreamReader reader = null;
            try {
                // 获得要上传的文件
                File file = new File(f.getAbsolutePath() , f.getName());
                reader = new InputStreamReader(new FileInputStream(file), "UTF-8");
            } catch (UnsupportedEncodingException e1) {
                e1.printStackTrace();
            } catch (FileNotFoundException e1) {
                e1.printStackTrace();
            }


            Map<String, String> map = MapUtil.newHashMap();
            BufferedReader bfreader = new BufferedReader(reader);
            String line = null;
            while (true) {
                try {
                    if (!((line = bfreader.readLine()) != null)) break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println(line);
            }

        });
    }


    @Test
    public void testListToMap(){
        List<String> beforeList = Lists.newArrayList();
        beforeList.add("before1");
        beforeList.add("before2");
        beforeList.add("before3");
        beforeList.add("before4");
        List<String> afterList = Lists.newArrayList();
        afterList.add("afterList1");
        afterList.add("afterList2");
        afterList.add("afterList3");
        afterList.add("afterList4");

//        Map<String,String> map = Maps.newLinkedHashMap();
//
//        for (int i = 0; i < beforeList.size(); i++) {
//            if(StringUtils.isNotBlank(beforeList.get(i)) &&StringUtils.isNotBlank(afterList.get(i))){
//                map.put(beforeList.get(i),afterList.get(i));
//            }
//        }
//
//        map.forEach((k,v)->{
//            System.out.println(k+":"+v);
//        });

        String join = String.join("\n", afterList);
        System.out.println(join);
    }
}
