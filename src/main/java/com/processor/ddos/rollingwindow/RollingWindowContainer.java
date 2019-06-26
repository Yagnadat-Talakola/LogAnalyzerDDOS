package com.processor.ddos.rollingwindow;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RollingWindowContainer {

    private Integer containerID;

    private Map<LocalDateTime, RollingWindow> rollingWindowContainerMap = new ConcurrentHashMap<>();

    public RollingWindowContainer(Integer partitionId) {
        this.containerID = partitionId;
    }

    public Map<LocalDateTime, RollingWindow> getRollingWindowContainerMap() {
        return rollingWindowContainerMap;
    }

    public Integer getContainerID() {
        return containerID;
    }
}
