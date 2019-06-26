package com.processor.ddos.operations;

import com.processor.ddos.model.ApacheLogEntry;
import com.processor.ddos.rollingwindow.RollingWindow;
import com.processor.ddos.rollingwindow.RollingWindowContainer;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Component
public class RollingWindowOperationsImpl implements RollingWindowOps {

    private static final Integer NUM_ACTIVE_BUCKETS = RETENTION_PERIOD / DURATION;
    private static final Logger logger = LoggerFactory.getLogger(RollingWindowOperationsImpl.class);

    @Override
    public RollingWindow getOrCreateRollingWindow(ApacheLogEntry message, RollingWindowContainer container) {

        Map<LocalDateTime, RollingWindow> map = container.getRollingWindowContainerMap();

        Optional<RollingWindow> rollingWindow = container.getRollingWindowContainerMap().entrySet().stream()
                .filter(x -> !x.getKey().isAfter(message.getTimestamp())
                        && x.getKey().plusSeconds(DURATION).isAfter(message.getTimestamp()))
                .map(x -> x.getValue()).findAny();

        if (!rollingWindow.isPresent()) {
            createRollingWindows(message.getTimestamp(), container);
            rollingWindow = Optional.of(map.get(message.getTimestamp()));
        }

        return rollingWindow.get();
    }

    public void updateRollingWindow(ApacheLogEntry msg, RollingWindow rw) {
        rw.incrementRequestCounter();
        rw.incrementIPRequestCounter(msg.getIpAddress());
        rw.incrementStatusCodeCounter(msg.getResponseStatusCode());
    }

    private void createRollingWindows(LocalDateTime newRWRequest, RollingWindowContainer container) {
        int i = 0;
        while (i <= NUM_ACTIVE_BUCKETS) {
            String rollingWindowID = RandomStringUtils.random(10, true, true);
            RollingWindow rw = new RollingWindow(container.getContainerID() + "-" + rollingWindowID, newRWRequest.plusSeconds(i * DURATION));
            logger.info("Created a rolling window {} ", rw.toString());
            container.getRollingWindowContainerMap().put(rw.getStartTS(), rw);
            i++;
        }
    }

    @Override
    public void purgeRollingWindows(RollingWindowContainer container) {

        Map<LocalDateTime, RollingWindow> map = container.getRollingWindowContainerMap();

        if (map.size() <= NUM_ACTIVE_BUCKETS) {
            logger.info("Nothing to purge");
            return;
        }

        Optional<LocalDateTime> maxTimestamp = map.entrySet().stream()
                .map(x -> x.getKey())
                .filter(Objects::nonNull)
                .max(Comparator.naturalOrder());

        LocalDateTime max = maxTimestamp.orElse(LocalDateTime.now());

        map.entrySet().stream()
                .filter(x -> x.getValue().getStartTS().isBefore(max.minusSeconds(RETENTION_PERIOD)))
                .forEach(x -> {
                    logger.info("Purging rolling window {}", x.getValue().getRollingWindowIdentifier());
                    map.remove(x.getKey(), x.getValue());
                });

    }



}
