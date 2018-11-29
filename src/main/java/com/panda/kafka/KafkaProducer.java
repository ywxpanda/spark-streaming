package com.panda.kafka;



import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import kafka.producer.ProducerConfig;

import java.util.Properties;

public class KafkaProducer extends Thread {
    private String topic;
    private Producer<Integer, String> producer;

    KafkaProducer(String topic) {
        this.topic = topic;
        //afka-console-consumer.sh --broker-list ***:9092 --topic ***
        Properties properties = new Properties();
        properties.put("metadata.broker.list", KafkaProperties.BROKER_LIST);
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        /*
         * 0:不会等待leader的握手信号，发出信号，不等待
         * 1：leader会将数据写入到一个log中，并返回一个信号（生产）
         * -1：leader会等待所有的副本中返回一个握手信号
         */
        properties.put("request.required.acks", "1");
        producer = new Producer<Integer, String>(new ProducerConfig(properties));
    }

    @Override
    public void run() {
        int msgNum = 0;
        while (true) {
            String msg = "msg:" + msgNum;
            producer.send(new KeyedMessage<Integer, String>(topic, msg));
            System.out.println("send:" + msg);
            msgNum++;
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
