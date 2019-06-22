package com.processor.ddos.model;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class RollingWindow {

    private LocalDateTime startTS;
    private LocalDateTime endTS;
    private Map<String, Integer> ipAddressCountMap;
    private Map<Integer, Integer> statusCodeCountMap;

    public RollingWindow(LocalDateTime startTS, LocalDateTime endTS) {
        this.startTS = startTS;
        this.endTS = endTS;
        this.ipAddressCountMap = new HashMap<>();
        this.statusCodeCountMap = new HashMap<>();
    }

    public void updateIPAddressMap(String ipAddress, Integer ipRequestCount) {
        int count = ipAddressCountMap.getOrDefault(ipAddress, 0);
        ipAddressCountMap.put(ipAddress, ipRequestCount + count);
    }

    public void updateStatusCodeMap(Integer statusCode, Integer totalCount) {
        int count = ipAddressCountMap.getOrDefault(statusCode, 0);
        statusCodeCountMap.put(statusCode, totalCount + count);
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

    public Map<Integer, Integer> getStatusCodeCountMap() {
        return statusCodeCountMap;
    }


}
