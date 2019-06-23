package com.processor.ddos.processor;

import com.processor.ddos.config.KafkaConsumerConfig;
import com.processor.ddos.model.RollingWindowObserver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class KafkaConsumerThreadFactory implements ApplicationRunner {

    @Autowired
    private KafkaConsumerConfig kafkaConsumerConfig;

    private static int PARTITION_COUNT = 1;

    @Override
    public void run(ApplicationArguments args) {
        ExecutorService es = Executors.newFixedThreadPool(PARTITION_COUNT * 2);
        for(int i = 0; i < PARTITION_COUNT; i++) {
            RollingWindowObserver rm = new RollingWindowObserver();
            MessageProcessor msg = new MessageProcessor(kafkaConsumerConfig, rm);
            es.submit(msg);
            es.submit(rm);
        }
    }
}
