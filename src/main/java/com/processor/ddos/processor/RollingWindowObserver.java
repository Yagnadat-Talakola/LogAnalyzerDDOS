package com.processor.ddos.processor;

import com.processor.ddos.model.WindowStatus;
import com.processor.ddos.model.ApacheLogEntry;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.processor.ddos.processor.RollingWindowInterface.DURATION;
import static com.processor.ddos.processor.RollingWindowInterface.RETENTION_PERIOD;

public class RollingWindowObserver implements Runnable {

    private static final Integer NUM_ACTIVE_BUCKETS = RETENTION_PERIOD / DURATION;

    private Map<LocalDateTime, RollingWindowImpl> rollingWindowBucketsMap = new ConcurrentHashMap<>();

    public void addEntry(ApacheLogEntry message) {

        Optional<RollingWindowImpl> rollingWindow = rollingWindowBucketsMap.entrySet().stream()
                .filter(x -> !x.getKey().isAfter(message.getTimestamp())
                             && x.getKey().plusSeconds(DURATION).isAfter(message.getTimestamp()))
                .map(x -> x.getValue()).findAny();

        if(!rollingWindow.isPresent()) {
            createRollingWindows(message.getTimestamp());
            rollingWindow = Optional.of(rollingWindowBucketsMap.get(message.getTimestamp()));
        }

        rollingWindow.get().addToWindow(message);
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
            RollingWindowImpl rw = new RollingWindowImpl(newRWRequest.plusSeconds(i * DURATION));
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
                        .map(x -> {
                            System.out.println(x.getValue().getWindowStatus() + " => rolling window => " + x.getValue().toString() );
                            return x;
                        })
                        .filter(x -> x.getValue().getWindowStatus() == WindowStatus.NOT_HEALTHY)
                        .forEach(x -> {
                            //write to results directory
                            System.out.println("Detecting IP Addresses with high traffic " + x.getValue().getDDOSInfoList());
                        });

                purgeRollingWindows();

            }
        } catch(Exception e) {
            e.printStackTrace();
        }

    }





}
