package com.processor.ddos.processor;

import com.google.common.collect.Iterables;
import com.processor.ddos.config.KafkaConsumerConfig;
import com.processor.ddos.model.ApacheLogEntry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class LogMessageProcessor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(LogMessageProcessor.class);

    private KafkaConsumerConfig config;
    private KafkaConsumer kafkaConsumer;
    private RollingWindowObserver windowObserver;

    public LogMessageProcessor(KafkaConsumerConfig consumerConfig, RollingWindowObserver windowObserver) {
        this.config = consumerConfig;
        this.windowObserver = windowObserver;
        this.kafkaConsumer = new KafkaConsumer<>(consumerConfig.createkafkaProp());
        this.kafkaConsumer.subscribe(Arrays.asList(this.config.getTopic()));
    }

    @Override
    public void run() {

        while (true) {

            ConsumerRecords<String, String> records = kafkaConsumer.poll(10);

            for (ConsumerRecord<String, String> record : records) {
                ApacheLogEntry logEntry = ApacheLogEntry.fromJson(record.value());
                this.windowObserver.addEntry(logEntry);
            }

            if(!records.isEmpty()) {
                ConsumerRecord<String, String> lastRec = Iterables.getLast(records);
                logger.info("Completed processing offset: " + lastRec.offset() + " on partition " + lastRec.partition());
            }

        }
    }




}
