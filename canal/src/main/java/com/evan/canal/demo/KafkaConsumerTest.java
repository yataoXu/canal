package com.evan.canal.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Description
 * @ClassName KafkaConsumerTest
 * @Author Evan
 * @date 2019.10.12 17:10
 */
public class KafkaConsumerTest implements Runnable {
    private final KafkaConsumer<String, String> consumer;
    private ConsumerRecords<String, String> msgList;
    private final String topic;
    private static final String GROUPID ="0";

    public KafkaConsumerTest(String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.2.196.18:9092,10.2.196.19:9092,10.2.196.20:9092");
        props.put("group.id", GROUPID);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer<String, String>(props);
        this.topic = topicName;
        this.consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        int messageNo = 1;
        System.out.println("---------开始消费---------");
        try {
            for (; ; ) {
                msgList = consumer.poll(1000);
                if (null != msgList && msgList.count() > 0) {
                    for (ConsumerRecord<String, String> record : msgList) {
                        System.out.println("=======receive: key ="+record.getClass() );
//                        //消费100条就打印 ,但打印的数据不一定是这个规律的
//                        if (messageNo % 100 == 0) {
//                            System.out.println(messageNo + "=======receive: key = " + record.key() + ", value = " + record.value() + " offset===" + record.offset());
//                        }
//                        //当消费了1000条就退出
//                        if (messageNo % 1000 == 0) {
//                            break;
//                        }
                        messageNo++;
                    }
                } else {
                    Thread.sleep(1000);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    public static void main(String args[]) {
        KafkaConsumerTest test1 = new KafkaConsumerTest("example");
        Thread thread1 = new Thread(test1);
        thread1.start();
    }
}
