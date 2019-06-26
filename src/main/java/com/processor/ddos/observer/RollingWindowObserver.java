package com.processor.ddos.observer;

import com.processor.ddos.operations.RollingWindowOps;
import com.processor.ddos.rollingwindow.RollingWindowStatus;
import com.processor.ddos.rollingwindow.RollingWindow;
import com.processor.ddos.rollingwindow.RollingWindowContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.util.*;


public class RollingWindowObserver implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RollingWindowObserver.class);

    private RollingWindowContainer container;
    private RollingWindowOps operations;

    public RollingWindowObserver(RollingWindowContainer container, RollingWindowOps operations) {
        this.container = container;
        this.operations = operations;
    }

    @Override
    public void run() {

        try {
            while (true) {
                Map<LocalDateTime, RollingWindow> map = container.getRollingWindowContainerMap();

                Thread.sleep(10000); //sleep for 10 seconds

                map.entrySet().stream()
                        .map(x -> {
                            logger.info(x.getValue().getRollingWindowStatus() + " => rolling window => " + x.getValue().toString());
                            return x;
                        })
                        .filter(x -> x.getValue().getRollingWindowStatus() == RollingWindowStatus.NOT_HEALTHY)
                        .forEach(x -> {
                            //write to results directory
                            List<String> ipAddrList = x.getValue().getDDOSInfoList();
                            writeToFile(ipAddrList);
                            logger.info("Detecting IP Addresses with high traffic " + x.getValue().getDDOSInfoList());
                        });

                operations.purgeRollingWindows(container);

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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
