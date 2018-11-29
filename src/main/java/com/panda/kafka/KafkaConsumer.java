package com.panda.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumer extends Thread {
    private String topic;

    KafkaConsumer(String topic) {
        this.topic = topic;

    }

    /**
     * 消费者：kafka-console-consumer.sh --zookeeper localhost:2181 --topic *** --from-beginning
     *
     * @return
     */
    private ConsumerConnector createConnector() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", KafkaProperties.ZK);
        properties.put("group.id",KafkaProperties.GROUP_ID);
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    @Override
    public void run() {
        ConsumerConnector consumer = createConnector();
        //构造消费当前consumer消费的topic
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(topic, 1);
        //获取对应消息的流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicMap);
        System.out.println("msg_nums: "+ messageStreams.get(topic).size());
        //获取当前的数据，最新的数据
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        while (iterator.hasNext()) {
            byte[] message = iterator.next().message();
            String s = new String(message);
            System.out.println("rec: " + s);
        }
    }
}
