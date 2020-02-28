package com.evan;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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
        BufferedReader bfreader = new BufferedReader(reader);
        String line;
        while ((line = bfreader.readLine()) != null) {
            remove(list1, line);
        }
        list1.stream().forEach(System.out::print);
    }


    @Test
    public void Test02() throws IOException {
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
        add(list1,list);
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
}
