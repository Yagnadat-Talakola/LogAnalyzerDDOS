package com.processor.ddos.processor;

import com.processor.ddos.model.WindowStatus;
import com.processor.ddos.model.ApacheLogEntry;

public interface RollingWindowInterface {

    String ERROR_STATUS = "503";
    int REQUEST_THRESHOLD = 25000;
    int RETENTION_PERIOD = 60;
    int ERROR_THRESHOLD_PERCENTAGE = 50;
    int DURATION = 30;

    void addToWindow(ApacheLogEntry logEntry);
    WindowStatus getWindowStatus();
}
