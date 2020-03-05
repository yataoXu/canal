package com.evan.config.property;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.jdo.annotations.Value;

/**
 * @Classname ConfigParam
 * @Description
 * @Date 2020/2/28 21:59
 * @Created by Evan
 */
@ConfigurationProperties(prefix = "dir")
@Data
public class ConfigParams {

    private String deletedDirMerge;
    private String deletedDirUpload;
    private String updateDirMerge;
    private String updateDirUpload;
    private String insertDirMerge;
    private String insertDirUpload;
    private String backup;


}
