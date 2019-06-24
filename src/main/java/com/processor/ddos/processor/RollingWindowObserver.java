package com.processor.ddos.processor;

import com.processor.ddos.model.WindowStatus;
import com.processor.ddos.model.ApacheLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.processor.ddos.processor.RollingWindowInterface.DURATION;
import static com.processor.ddos.processor.RollingWindowInterface.RETENTION_PERIOD;

public class RollingWindowObserver implements Runnable {

    private static final Integer NUM_ACTIVE_BUCKETS = RETENTION_PERIOD / DURATION;
    private static final Logger logger = LoggerFactory.getLogger(RollingWindowObserver.class);

    private Map<LocalDateTime, RollingWindowImpl> rollingWindowBucketsMap = new ConcurrentHashMap<>();

    public void addEntry(ApacheLogEntry message) {

        Optional<RollingWindowImpl> rollingWindow = rollingWindowBucketsMap.entrySet().stream()
                .filter(x -> !x.getKey().isAfter(message.getTimestamp())
                        && x.getKey().plusSeconds(DURATION).isAfter(message.getTimestamp()))
                .map(x -> x.getValue()).findAny();

        if (!rollingWindow.isPresent()) {
            createRollingWindows(message.getTimestamp());
            rollingWindow = Optional.of(rollingWindowBucketsMap.get(message.getTimestamp()));
        }

        rollingWindow.get().addToWindow(message);
    }

    public void purgeRollingWindows() {


        if (rollingWindowBucketsMap.size() <= NUM_ACTIVE_BUCKETS) {
            logger.info("Nothing to purge");
            return;
        }

        Optional<LocalDateTime> maxTimestamp = rollingWindowBucketsMap.entrySet().stream()
                .map(x -> x.getKey())
                .filter(Objects::nonNull)
                .max(Comparator.naturalOrder());

        LocalDateTime max = maxTimestamp.orElse(LocalDateTime.now());

        rollingWindowBucketsMap.entrySet().stream()
                .filter(x -> x.getValue().getStartTS().isBefore(max.minusSeconds(DURATION)))
                .forEach(x -> {
                    logger.info("Purging rolling window " + x.getValue().toString());
                    rollingWindowBucketsMap.remove(x.getKey(), x.getValue());
                });

    }

    public void createRollingWindows(LocalDateTime newRWRequest) {
        int i = 0;
        while (i <= NUM_ACTIVE_BUCKETS) {
            RollingWindowImpl rw = new RollingWindowImpl(newRWRequest.plusSeconds(i * DURATION));
            logger.info("Created a rolling window " + rw.toString());
            rollingWindowBucketsMap.put(rw.getStartTS(), rw);
            i++;
        }
    }

    @Override
    public void run() {

        try {
            while (true) {

                Thread.sleep(10000);
                rollingWindowBucketsMap.entrySet().stream()
                        .map(x -> {
                            logger.info(x.getValue().getWindowStatus() + " => rolling window => " + x.getValue().toString());
                            return x;
                        })
                        .filter(x -> x.getValue().getWindowStatus() == WindowStatus.NOT_HEALTHY)
                        .forEach(x -> {
                            //write to results directory
                            List<String> ipAddrList = x.getValue().getDDOSInfoList();
                            writeToFile(ipAddrList);
                            logger.info("Detecting IP Addresses with high traffic " + x.getValue().getDDOSInfoList());
                        });

                purgeRollingWindows();

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void writeToFile(List<String> ipList) {
        try {
            FileWriter fw = new FileWriter("./results", true);
            BufferedWriter bw = new BufferedWriter(fw);
            for (String ipAddr : ipList) {
                bw.write(ipAddr);
                bw.newLine();
            }
            bw.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }


}
