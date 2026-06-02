package com.karix.dlrreceiver.util;

import com.karix.commonutil.enums.Channel;
import com.karix.dlrreceiver.cache.GriffinFlagCacheManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class FlowRouter {

    private static final Logger log = LogManager.getLogger(FlowRouter.class);

    public enum Flow {
        GRIFFIN,
        BILLING
    }

    private final GriffinFlagCacheManager griffinFlagCacheManager;

    public FlowRouter(GriffinFlagCacheManager griffinFlagCacheManager) {
        this.griffinFlagCacheManager = griffinFlagCacheManager;
    }

    public Flow route(Channel channel, String ackId) {
        if (ackId == null || ackId.isBlank()) {
            log.warn("route() called with blank ackId channel={}, defaulting to BILLING", channel);
            return Flow.BILLING;
        }

        Optional<String> flag = griffinFlagCacheManager.get(channel, ackId);

        if (flag.isPresent() && "1".equals(flag.get())) {
            log.info("channel={} ackId={} → GRIFFIN flow (griffinUploadRequired=1)", channel, ackId);
            return Flow.GRIFFIN;
        }

        if (log.isDebugEnabled()) {
            log.info("channel={} ackId={} → BILLING flow (griffinUploadRequired={})",
                    channel, ackId, flag.orElse("absent"));
        }
        return Flow.BILLING;
    }
}