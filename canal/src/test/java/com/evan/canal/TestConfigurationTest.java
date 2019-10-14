package com.evan.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;
import java.util.List;


/**
 * @Description
 * @ClassName TestConfiguration
 * @Author Evan
 * @date 2019.10.13 21:02
 */
public class TestConfigurationTest {


        public static void main(String args[]) {
            // 创建链接
            CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("10.2.196.24", 11111),"example", "root", "Qwe123!@#");

//            CanalConnector connector = CanalConnectors.newClusterConnector("10.2.196.18:2181,10.2.196.19:2181,10.2.196.20:2181", "example", "root", "Qwe123!@#");
            int batchSize = 1000;
            int emptyCount = 0;
            try {
                connector.connect();
                connector.subscribe(".*\\..*");
                connector.rollback();
                int totalEmtryCount = 1200;
                while (emptyCount < totalEmtryCount) {
                    Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        emptyCount++;
//                    System.out.println("empty count : " + emptyCount);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } else {
                        emptyCount = 0;
                        // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                        printEntryToKafka(message.getEntries());
                    }

                    connector.ack(batchId); // 提交确认
                    // connector.rollback(batchId); // 处理失败, 回滚数据
                }

                System.out.println("empty too many times, exit");
            } finally {
                connector.disconnect();
            }
        }

        private static void printEntryToKafka(List<Entry> entrys) {
            for (Entry entry : entrys) {
                if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN
                        || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                    continue;
                }

                CanalEntry.RowChange rowChage = null;
                try {
                    rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                            e);
                }

                CanalEntry.EventType eventType = rowChage.getEventType();
                System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                        entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                        entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), eventType));

                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    if (eventType == CanalEntry.EventType.DELETE) {
                        printColumn(rowData.getBeforeColumnsList());
                    } else if (eventType == CanalEntry.EventType.INSERT) {
                        printColumn(rowData.getAfterColumnsList());
                    } else {
                        System.out.println("-------> before");
                        printColumn(rowData.getBeforeColumnsList());
                        System.out.println("-------> after");
                        printColumn(rowData.getAfterColumnsList());
                    }
                }
            }
        }

        private static void printColumn(List<CanalEntry.Column> columns) {
            for (CanalEntry.Column column : columns) {
                System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
            }
        }

    }
