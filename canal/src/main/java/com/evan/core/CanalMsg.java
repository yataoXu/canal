package com.evan.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description Canal 的一些信息
 * @ClassName core
 * @Author Evan
 * @date 2019.10.14 13:33
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CanalMsg {
    /**
     * 指令
     */
    private String destination;
    /**
     * 数据库实例名称
     */
    private String schemaName;
    /**
     * 数据库表名称
     */
    private String tableName;
}
