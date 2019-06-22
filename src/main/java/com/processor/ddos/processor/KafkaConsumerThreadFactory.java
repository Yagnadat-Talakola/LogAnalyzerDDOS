package com.processor.ddos.processor;

import com.processor.ddos.config.KafkaConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class KafkaConsumerThreadFactory implements ApplicationRunner {

    @Autowired
    private KafkaConsumerConfig kafkaConsumerConfig;

    private static int PARTITION_COUNT = 1;

    @Override
    public void run(ApplicationArguments args) {
        ExecutorService es = Executors.newFixedThreadPool(PARTITION_COUNT);
        for(int i = 0; i < PARTITION_COUNT; i++) {
            MessageProcessor msg = new MessageProcessor(kafkaConsumerConfig);
            es.submit(msg);
        }

    }
}
