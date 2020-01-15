package com.github.kafka.Producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //create producer properties
        final Logger logger= LoggerFactory.getLogger(ProducerDemoKeys.class);
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //create producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        //create a producer record
    for(int i=0; i<10; i++) {
        String topic="new_topic";
        String value="kafka topic is created   " + i;
        String key="id_"+ i;
        logger.info("key: "+key);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value );
        //send data

        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    logger.info("Metadata recieved. \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partitions: " + recordMetadata.partition() + "\n" +
                            "Offset:" + recordMetadata.offset());
                } else {
                    logger.error("failed while generating data", e);
                }
            }
        }).get();
    }
        producer.flush();
        producer.close();
    }
}
