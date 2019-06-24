package com.processor.ddos.processor;

import com.processor.ddos.model.WindowStatus;
import com.processor.ddos.model.ApacheLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RollingWindowImpl implements RollingWindowInterface {

    private LocalDateTime startTS;
    private LocalDateTime endTS;
    private Integer totalRequestCount;
    private Map<String, Integer> ipAddressCountMap;
    private Map<String, Integer> statusCodeCountMap;
    private static final Logger logger = LoggerFactory.getLogger(RollingWindowImpl.class);

    public RollingWindowImpl(LocalDateTime startTS) {
        this.startTS = startTS;
        this.endTS = startTS.plusSeconds(DURATION);
        this.ipAddressCountMap = new HashMap<>();
        this.statusCodeCountMap = new HashMap<>();
        this.totalRequestCount = 0;
    }

    @Override
    public void addToWindow(ApacheLogEntry msg) {

        ++totalRequestCount;

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

    @Override
    public WindowStatus getWindowStatus() {
        int status503Count = statusCodeCountMap.getOrDefault(ERROR_STATUS, 0);
        if(totalRequestCount == 0) return WindowStatus.HEALTHY;

        float status503Percentage =  ((float)status503Count / totalRequestCount) * 100;
        logger.info("Error threshold percentage is " + status503Percentage);

        if(totalRequestCount >= REQUEST_THRESHOLD && status503Percentage > ERROR_THRESHOLD_PERCENTAGE) {
            return WindowStatus.NOT_HEALTHY;
        }
        return WindowStatus.HEALTHY;
    }


    public List<String> getDDOSInfoList() {
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
                ", statusCodeCountMap=" + statusCodeCountMap +
                '}';
    }
}
