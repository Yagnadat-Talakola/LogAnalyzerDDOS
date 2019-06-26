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
    private KafkaConsumer consumer;
    private RollingWindowOps operations;
    private RollingWindowContainer container;

    public LogMessageConsumer(KafkaConsumerConfig consumerConfig, RollingWindowContainer container, RollingWindowOps operations) {
        this.config = consumerConfig;
        this.operations = operations;
        this.consumer = new KafkaConsumer<>(consumerConfig.createkafkaProp());
        this.consumer.subscribe(Arrays.asList(this.config.getTopic()));
        this.container = container;
    }

    @Override
    public void run() {

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(10);

            for (ConsumerRecord<String, String> record : records) {
                ApacheLogEntry logEntry = ApacheLogEntry.fromJson(record.value());
                RollingWindow rw = operations.getOrCreateRollingWindow(logEntry, container);
                operations.updateRollingWindow(logEntry, rw);
            }

            if(!records.isEmpty()) {
                ConsumerRecord<String, String> lastRec = Iterables.getLast(records);
                logger.info("Completed processing offset: {} on partition {}", lastRec.offset(), lastRec.partition());
            }

        }
    }




}
