package com.evan;

import com.evan.service.HiveSqlBuilderService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @Classname TestHiveSql
 * @Description
 * @Date 2020/2/28 15:42
 * @Created by Evan
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class TestHiveSql {

    @Autowired
    HiveSqlBuilderService hiveSqlBuilderService;

    @Test
    public void TestGetDataByTableName(){
    }

}
