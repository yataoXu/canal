package com.evan;

import org.junit.Test;

/**
 * @Classname TestSql
 * @Description
 * @Date 2020/3/6 9:30
 * @Created by Evan
 */
public class TestSql {

    @Test
    public void test01(){
        String mysqlSql = "create table `phone`( `id` int, `name` varchar(255), `price` int, primary key(`id`) )";

        String dropsql1 = "DROP TABLE orders_by_date";
        String dropsql2=   "DROP TABLE IF EXISTS orders_by_date";
    }



}
