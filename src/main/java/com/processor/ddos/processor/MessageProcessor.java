package com.processor.ddos.processor;

import com.processor.ddos.config.KafkaConsumerConfig;
import com.processor.ddos.model.ApacheLogTemplate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

public class MessageProcessor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);

    private KafkaConsumerConfig config;
    private KafkaConsumer kafkaConsumer;

    public MessageProcessor(KafkaConsumerConfig consumerConfig) {
        this.config = consumerConfig;
        this.kafkaConsumer = new KafkaConsumer<>(consumerConfig.createkafkaProp());
        this.kafkaConsumer.subscribe(Arrays.asList(this.config.getTopic()));
    }

    @Override
    public void run() {
        //process your messages
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(10);
            List<ApacheLogTemplate> msgList = new ArrayList<>();
            for (ConsumerRecord<String, String> record : records) {
                ApacheLogTemplate logTemplate = ApacheLogTemplate.fromJson(record.value());
                msgList.add(logTemplate);
                logger.info(logTemplate.toString());
            }

            if (!msgList.isEmpty()) {
                // sorted map of IP -> count
                sortMsgByIPCount(msgList);

                // list of IPs with status code != 200
                List<ApacheLogTemplate> failList = msgList.stream().filter(x -> x.getResponseStatusCode() != String.valueOf(200)).collect(Collectors.toList());

                // create a sorted map of failed requests by count
                sortMsgByIPCount(failList);
            }
        }
    }

    /*
       Bot finder:
       1. In a volumetric attack, bot(s) typically tend to bombard the server with greater than average traffic.
          An assumption is being made here that the highest count could most probably came from a bot. This can be further validated with
          lower than average time duration between requests.

       2. Another way to find bot activity is to find highest count of non-200 status codes being returned by the server.

     */

    public Map<String, Integer> sortMsgByIPCount(List<ApacheLogTemplate> msgList) {
        // iterate through the list and map IP to count.
        Map<String, Integer> ipCountMap = new HashMap<>();
        msgList.stream().forEach(x -> {
            int count = ipCountMap.getOrDefault(x.getIpAddress(), 0);
            ipCountMap.put(x.getIpAddress(), count + 1);
        });

        // sort ipCount by asc value
        Map<String, Integer> sorted = ipCountMap
                .entrySet()
                .stream()
                .sorted(comparingByValue())
                .collect(toMap(e -> e.getKey(), e -> e.getValue(), (e1, e2) -> e2,
                                LinkedHashMap::new));
        return sorted;
    }

}
