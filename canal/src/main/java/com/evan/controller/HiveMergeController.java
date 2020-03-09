package com.evan.controller;

import com.evan.service.DataMergeService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @Classname HiveMergeController
 * @Description
 * @Date 2020/3/2 10:41
 * @Created by Evan
 */
@Slf4j
@RestController
@RequestMapping("/merge")
@RequiredArgsConstructor(onConstructor_ = @Autowired)
public class HiveMergeController {

    private final DataMergeService dataMergeService;

    @GetMapping(value = "insert/{databaseName}")
    public String insertMerge(@PathVariable("databaseName") String databaseName) {

        try {
            dataMergeService.dataMergeInsert(databaseName);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "success";
    }


    @GetMapping(value = "update/{databaseName}")
    public String updateMerge(@PathVariable("databaseName") String databaseName) {

        try {
            dataMergeService.dataMergeUpdate(databaseName);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "success";
    }

    @GetMapping(value = "delete/{databaseName}")
    public String deleteMerge(@PathVariable("databaseName") String databaseName) {

        try {
            dataMergeService.dataMergeDelete(databaseName);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "success";
    }

    @GetMapping(value = "total/{databaseName}")
    public String totalMerge(@PathVariable("databaseName") String databaseName) {

        try {
            dataMergeService.dataMergeDelete(databaseName);
            dataMergeService.dataMergeUpdate(databaseName);
            dataMergeService.dataMergeInsert(databaseName);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "success";
    }

}
