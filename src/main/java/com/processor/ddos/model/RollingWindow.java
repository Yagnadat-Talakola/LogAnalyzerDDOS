package com.processor.ddos.model;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RollingWindow {

    private static final String ERROR_STATUS = "503";
    public static final int REQUEST_THRESHOLD = 250000;
    public static final int ERROR_THRESHOLD_PERCENTAGE = 75;
    private static Integer DURATION = 30;

    private LocalDateTime startTS;
    private LocalDateTime endTS;
    private Integer totalRequestCount;
    private Map<String, Integer> ipAddressCountMap;
    private Map<String, Integer> statusCodeCountMap;

    public RollingWindow(LocalDateTime startTS) {
        this.startTS = startTS;
        this.endTS = startTS.plusSeconds(DURATION);
        this.ipAddressCountMap = new HashMap<>();
        this.statusCodeCountMap = new HashMap<>();
        totalRequestCount = 0;
    }

    public void updateCounts(ApacheLogTemplate msg) {

        totalRequestCount++;

        int ipAddrCount = ipAddressCountMap.getOrDefault(msg.getIpAddress(), 0);
        ipAddressCountMap.put(msg.getIpAddress(), ++ipAddrCount);

        int statusCodeCount = statusCodeCountMap.getOrDefault(msg.getResponseStatusCode(), 0);
        statusCodeCountMap.put(msg.getResponseStatusCode(), ++statusCodeCount);
    }

    public LocalDateTime getStartTS() {
        return startTS;
    }

    public LocalDateTime getEndTS() {
        return endTS;
    }

    public Map<String, Integer> getIpAddressCountMap() {
        return ipAddressCountMap;
    }

    public Map<String, Integer> getStatusCodeCountMap() {
        return statusCodeCountMap;
    }

    public Integer getTotalRequestCount() {
        return totalRequestCount;
    }

    public AlertStatus getStatus() {
        int status503Count = statusCodeCountMap.getOrDefault(ERROR_STATUS, 0);
        if(totalRequestCount == 0) return AlertStatus.HEALTHY;
        int status503Percentage =  (status503Count / totalRequestCount) * 100;
        if(totalRequestCount >= REQUEST_THRESHOLD && status503Percentage > ERROR_THRESHOLD_PERCENTAGE) {
            return AlertStatus.NOT_HEALTHY;
        }
        return AlertStatus.HEALTHY;
    }

    public List<String> getHighTrafficIPAddr() {
        int requestThresholdPerIPAddress = REQUEST_THRESHOLD / DURATION;
        return ipAddressCountMap.entrySet().stream()
                .filter(x -> x.getValue() >= requestThresholdPerIPAddress)
                .map(x -> x.getKey())
                .collect(Collectors.toList());
    }

    @Override
    public java.lang.String toString() {
        return "RollingWindow{" +
                "startTS=" + startTS +
                ", endTS=" + endTS +
                ", totalRequestCount=" + totalRequestCount +
                ", ipAddressCountMap=" + ipAddressCountMap +
                ", statusCodeCountMap=" + statusCodeCountMap +
                '}';
    }
}
