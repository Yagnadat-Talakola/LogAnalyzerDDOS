package com.processor.ddos.processor;

import com.processor.ddos.config.KafkaConsumerConfig;
import com.processor.ddos.model.ApacheLogTemplate;
import com.processor.ddos.model.RollingWindowObserver;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class MessageProcessor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);

    private KafkaConsumerConfig config;
    private KafkaConsumer kafkaConsumer;
    private RollingWindowObserver manager;

    public MessageProcessor(KafkaConsumerConfig consumerConfig, RollingWindowObserver rm) {
        this.config = consumerConfig;
        this.manager = rm;
        this.kafkaConsumer = new KafkaConsumer<>(consumerConfig.createkafkaProp());
        this.kafkaConsumer.subscribe(Arrays.asList(this.config.getTopic()));
    }

    @Override
    public void run() {

        //process your messages
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(10);

            for (ConsumerRecord<String, String> record : records) {
                ApacheLogTemplate logTemplate = ApacheLogTemplate.fromJson(record.value());
                this.manager.addEntry(logTemplate);
            }

        }
    }




}
