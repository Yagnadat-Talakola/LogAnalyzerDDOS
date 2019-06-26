package com.processor.ddos.operations;

import com.processor.ddos.model.ApacheLogEntry;
import com.processor.ddos.rollingwindow.RollingWindow;
import com.processor.ddos.rollingwindow.RollingWindowContainer;


public interface RollingWindowOps {

    String ERROR_STATUS = "503";
    int REQUEST_THRESHOLD = 25000; //request count threshold

    int RETENTION_PERIOD = 300; // (in seconds) retention duration of the container
    int ERROR_THRESHOLD_PERCENTAGE = 50; //503 status code percentage
    int DURATION = 30; // (in seconds)

    RollingWindow getOrCreateRollingWindow(ApacheLogEntry message, RollingWindowContainer container);
    void updateRollingWindow(ApacheLogEntry msg, RollingWindow rw);
    void purgeRollingWindows(RollingWindowContainer container);
}
