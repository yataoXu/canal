package com.evan.canal.demo;

/**
 * @Description
 * @ClassName KafkaProduct
 * @Author Evan
 * @date 2019.10.12 17:09
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProduct implements Runnable {
    public final KafkaProducer<String, String> producer;
    public final String topic;

    public KafkaProduct(String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.2.196.19:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("client.id", "producer.client.id.demo");
        props.put("acks", "all");
        props.put("retries", "0");
        props.put("batch.size", 16384);
        this.producer = new KafkaProducer<String, String>(props);
        this.topic = topicName;
    }

    @Override
    public void run() {
        int messageNo = 1;
        try {
            for (; ; ) {
                String messageStr = "this is " + messageNo + "data";
                producer.send(new ProducerRecord<String, String>(topic, "Message", messageStr));
                // 生产数据
                if (messageNo % 1000 == 0) {
                    System.out.println("send data" + messageStr);
                }
                if (messageNo % 1000 == 0) {
                    System.out.println("success send data" + messageStr);
                    break;
                }
                messageNo++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public static void main(String[] args) {
        KafkaProduct test = new KafkaProduct("test");
        Thread thread = new Thread(test);
        thread.start();
    }
}
