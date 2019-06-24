package com.processor.ddos.processor;

import com.processor.ddos.config.KafkaConsumerConfig;
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

    @Override
    public void run(ApplicationArguments args) {

        int partitionCount = kafkaConsumerConfig.getConcurrency();
        ExecutorService es = Executors.newFixedThreadPool(partitionCount * 2);

        for(int i = 0; i < partitionCount; i++) {
            RollingWindowObserver rm = new RollingWindowObserver();
            LogMessageProcessor msg = new LogMessageProcessor(kafkaConsumerConfig, rm);
            es.submit(msg);
            es.submit(rm);
        }

    }
}
