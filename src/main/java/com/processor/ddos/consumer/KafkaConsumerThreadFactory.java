package com.processor.ddos.consumer;

import com.processor.ddos.config.KafkaConsumerConfig;
import com.processor.ddos.operations.RollingWindowOps;
import com.processor.ddos.rollingwindow.RollingWindowContainer;
import com.processor.ddos.observer.RollingWindowObserver;
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

    @Autowired
    private RollingWindowOps rollingWindowOperations;

    @Override
    public void run(ApplicationArguments args) {

        int partitionCount = kafkaConsumerConfig.getConcurrency();
        ExecutorService es = Executors.newFixedThreadPool(partitionCount * 2);

        for(int partitionID = 0; partitionID < partitionCount; partitionID++) {
            RollingWindowContainer container = new RollingWindowContainer(partitionID);
            RollingWindowObserver rollingWindowObserver = new RollingWindowObserver(container, rollingWindowOperations);
            LogMessageConsumer msg = new LogMessageConsumer(kafkaConsumerConfig, rollingWindowOperations, container);
            es.submit(msg);
            es.submit(rollingWindowObserver);
        }

    }
}
