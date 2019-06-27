package com.processor.ddos.rollingwindow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.processor.ddos.operations.RollingWindowOps.*;

public class RollingWindow {

    private String identifier;
    private LocalDateTime startTS;
    private LocalDateTime endTS;
    private Integer totalRequestCount;
    private Map<String, Integer> ipAddressCountMap;
    private Map<String, Integer> statusCodeCountMap;

    private static final Logger logger = LoggerFactory.getLogger(RollingWindow.class);

    public RollingWindow(String rollingWindowIdentifier, LocalDateTime startTS) {
        this.identifier = rollingWindowIdentifier;
        this.startTS = startTS;
        this.endTS = startTS.plusSeconds(DURATION);
        this.ipAddressCountMap = new HashMap<>();
        this.statusCodeCountMap = new HashMap<>();
        this.totalRequestCount = 0;
    }

    public LocalDateTime getStartTS() {
        return startTS;
    }

    public LocalDateTime getEndTS() {
        return endTS;
    }

    public Integer getTotalRequestCount() {
        return totalRequestCount;
    }

    public Map<String, Integer> getIpAddressCountMap() {
        return ipAddressCountMap;
    }

    public Map<String, Integer> getStatusCodeCountMap() {
        return statusCodeCountMap;
    }

    public String getRollingWindowIdentifier() {
        return identifier;
    }

    public void incrementRequestCounter() {
        totalRequestCount++;
    }

    public void incrementIPRequestCounter(String ipAddress) {
        int ipAddrCount = ipAddressCountMap.getOrDefault(ipAddress, 0);
        ipAddressCountMap.put(ipAddress, ++ipAddrCount);
    }

    public void incrementStatusCodeCounter(String statusCode) {
        int ipAddrCount = statusCodeCountMap.getOrDefault(statusCode, 0);
        statusCodeCountMap.put(statusCode, ++ipAddrCount);
    }

    public RollingWindowStatus getRollingWindowStatus() {

        if(getTotalRequestCount() == 0) return RollingWindowStatus.HEALTHY;

        int status503Count = getStatusCodeCountMap().getOrDefault(ERROR_STATUS, 0);

        float status503Percentage =  ((float)status503Count / getTotalRequestCount()) * 100;
        logger.info("Error threshold percentage for {} is {}", identifier,  status503Percentage);
        logger.info("Request volume for {} is {}", identifier, getTotalRequestCount());

        if(getTotalRequestCount() >= REQUEST_THRESHOLD && status503Percentage > ERROR_THRESHOLD_PERCENTAGE) {
            return RollingWindowStatus.NOT_HEALTHY;
        }
        return RollingWindowStatus.HEALTHY;
    }

    public List<String> getDDOSInfoList() {
        return ipAddressCountMap.entrySet().stream()
                .filter(x -> x.getValue() >= REQUEST_THRESHOLD)
                .map(x -> x.getKey())
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "RollingWindow{" +
                "rollingWindowIdentifier='" + identifier + '\'' +
                ", startTS=" + startTS +
                ", endTS=" + endTS +
                ", totalRequestCount=" + totalRequestCount +
                ", ipAddressCountMap=" + ipAddressCountMap +
                ", statusCodeCountMap=" + statusCodeCountMap +
                '}';
    }
}
