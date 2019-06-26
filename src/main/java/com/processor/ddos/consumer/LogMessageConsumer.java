package com.processor.ddos.consumer;

import com.google.common.collect.Iterables;
import com.processor.ddos.config.KafkaConsumerConfig;
import com.processor.ddos.model.ApacheLogEntry;
import com.processor.ddos.operations.RollingWindowOps;
import com.processor.ddos.rollingwindow.RollingWindow;
import com.processor.ddos.rollingwindow.RollingWindowContainer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class LogMessageConsumer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(LogMessageConsumer.class);

    private KafkaConsumerConfig config;
    private KafkaConsumer kafkaConsumer;
    private RollingWindowOps rollingWindowOperations;
    private RollingWindowContainer container;

    public LogMessageConsumer(KafkaConsumerConfig consumerConfig, RollingWindowOps rollingWindowOperations, RollingWindowContainer container) {
        this.config = consumerConfig;
        this.rollingWindowOperations = rollingWindowOperations;
        this.kafkaConsumer = new KafkaConsumer<>(consumerConfig.createkafkaProp());
        this.kafkaConsumer.subscribe(Arrays.asList(this.config.getTopic()));
        this.container = container;
    }

    @Override
    public void run() {

        while (true) {

            ConsumerRecords<String, String> records = kafkaConsumer.poll(10);

            for (ConsumerRecord<String, String> record : records) {
                ApacheLogEntry logEntry = ApacheLogEntry.fromJson(record.value());
                RollingWindow rw = rollingWindowOperations.getOrCreateRollingWindow(logEntry, container);
                rollingWindowOperations.updateRollingWindow(logEntry, rw);
            }

            if(!records.isEmpty()) {
                ConsumerRecord<String, String> lastRec = Iterables.getLast(records);
                logger.info("Completed processing offset: " + lastRec.offset() + " on partition " + lastRec.partition());
            }

        }
    }




}
