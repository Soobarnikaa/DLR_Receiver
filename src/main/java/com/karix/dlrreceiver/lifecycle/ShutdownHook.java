package com.karix.dlrreceiver.lifecycle;

import com.karix.dlrreceiver.bootstrap.BillingRetryWorker;
import com.karix.dlrreceiver.bootstrap.DlrWorker;
import com.karix.nats.core.service.GenericInitializer;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ShutdownHook {

    private static final Logger logger =
            LoggerFactory.getLogger(ShutdownHook.class);

    private final DlrWorker dlrWorker;
    private final BillingRetryWorker billingRetryWorker;

    public ShutdownHook(DlrWorker dlrWorker,
                        BillingRetryWorker billingRetryWorker) {
        this.dlrWorker = dlrWorker;
        this.billingRetryWorker = billingRetryWorker;
    }

    @PreDestroy
    public void shutdown() {

        logger.warn("DLR Receiver shutdown initiated");

        dlrWorker.stop();
        billingRetryWorker.stop();

        logger.info("DLR workers stopping gracefully");

        try {

            logger.warn("Shutting down NATS pool");

            GenericInitializer.getPoolManager().shutdown();

        } catch (Exception e) {

            logger.error("Error shutting down NATS pool", e);
        }

        logger.warn("DLR Receiver shutdown complete");
    }
}