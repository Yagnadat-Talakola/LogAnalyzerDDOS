package com.processor.ddos.model;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class RollingWindowObserver implements Runnable {

    private static final Integer RETENTION_PERIOD = 60; //in seconds
    private static final Integer DURATION = 30; //in seconds
    private static final Integer NUM_ACTIVE_BUCKETS = RETENTION_PERIOD / DURATION;

    private Map<LocalDateTime, RollingWindow> rollingWindowBucketsMap = new ConcurrentHashMap<>();

    public void addEntry(ApacheLogTemplate message) {

        Optional<RollingWindow> rw = rollingWindowBucketsMap.entrySet().stream()
                .filter(x -> !x.getKey().isAfter(message.getTimestamp())
                             && x.getKey().plusSeconds(DURATION).isAfter(message.getTimestamp()))
                .map(x -> x.getValue()).findAny();

        if(!rw.isPresent()) {
            createRollingWindows(message.getTimestamp());
            rw = Optional.of(rollingWindowBucketsMap.get(message.getTimestamp()));
        }

        rw.get().updateCounts(message);
    }

    public void purgeRollingWindows() {

        System.out.println("purge rolling windows");

        if(rollingWindowBucketsMap.size() <= NUM_ACTIVE_BUCKETS) {
            return;
        }

        Optional<LocalDateTime> maxTimestamp = rollingWindowBucketsMap.entrySet().stream()
                .map(x -> x.getKey())
                .filter(Objects::nonNull)
                .max(Comparator.naturalOrder());

        LocalDateTime max = maxTimestamp.orElse(LocalDateTime.now());

        rollingWindowBucketsMap.entrySet().stream()
                .filter(x -> x.getValue().getStartTS().isBefore(max.minusSeconds(30)))
                .forEach(x -> {
                    System.out.println("purging " + x.getValue().toString());
                    rollingWindowBucketsMap.remove(x.getKey(), x.getValue());
         });

    }

    public void createRollingWindows(LocalDateTime newRWRequest) {
        int i = 0;
        while(i <= NUM_ACTIVE_BUCKETS) {
            RollingWindow rw = new RollingWindow(newRWRequest.plusSeconds(i * DURATION));
            System.out.println("Created a rolling window " + rw.toString());
            rollingWindowBucketsMap.put(rw.getStartTS(), rw);
            i++;
        }
    }

    @Override
    public void run() {

        try {
            while (true) {

                Thread.sleep(30000);

                rollingWindowBucketsMap.entrySet().stream()
                        .filter(x -> x.getValue().getStatus() == AlertStatus.NOT_HEALTHY)
                        .forEach(x -> {
                            //write to results directory
                            System.out.println("Detecting IP Addresses with high traffic " + x.getValue().getHighTrafficIPAddr());
                        });

                purgeRollingWindows();

            }
        } catch(Exception e) {
            e.printStackTrace();
        }

    }





}
