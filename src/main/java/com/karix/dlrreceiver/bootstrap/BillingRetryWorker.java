package com.karix.dlrreceiver.bootstrap;

import com.karix.commonutil.model.GriffinUploaderNATSResponseModel;
import com.karix.dlrreceiver.util.NatsPublisherHelper;
import jakarta.annotation.PostConstruct;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.LinkedBlockingQueue;

@Component
public class BillingRetryWorker {

    private static final Logger log = LogManager.getLogger(BillingRetryWorker.class);

    @Value("${billing.retry.delay.ms:5000}")
    private long retryDelayMs;

    private final NatsPublisherHelper natsPublisherHelper;

    private final LinkedBlockingQueue<RetryTask> queue = new LinkedBlockingQueue<>();

    private volatile boolean running = true;
    private Thread workerThread;

    public BillingRetryWorker(NatsPublisherHelper natsPublisherHelper) {
        this.natsPublisherHelper = natsPublisherHelper;
    }

    @PostConstruct
    public void start() {
        workerThread = new Thread(this::retryLoop, "billing-retry-worker");
        workerThread.setDaemon(false);
        workerThread.start();
        log.info("BillingRetryWorker started");
    }

    public void stop() {
        log.debug("Stopping BillingRetryWorker (pending={})", queue.size());
        running = false;
        if (workerThread != null && workerThread.isAlive()) {
            workerThread.interrupt();
            try {
                workerThread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for BillingRetryWorker to stop");
            }
        }
        log.info("BillingRetryWorker stopped");
    }

    public void enqueue(String subject,
                        GriffinUploaderNATSResponseModel payload,
                        String ackId) {

        RetryTask task = new RetryTask(subject, payload, ackId);
        boolean enqueued = queue.offer(task);

        if (enqueued) {
            log.warn("BILLING queued for retry ackId={} subject={} pendingTotal={}",
                    ackId, subject, queue.size());
        } else {
            log.error("BILLING retry queue full, message lost ackId={} subject={} payload={}",
                    ackId, subject, payload);
        }
    }

    private void retryLoop() {
        log.debug("BillingRetryWorker loop running thread={}", Thread.currentThread().getName());

        while (running) {
            try {
                RetryTask task = queue.take();

                boolean success = attempt(task);

                if (success) {
                    log.debug("BILLING retry succeeded ackId={} subject={}", task.ackId, task.subject);
                } else {
                    log.warn("BILLING retry failed ackId={} subject={} — re-enqueuing, backing off {}ms",
                            task.ackId, task.subject, retryDelayMs);

                    boolean requeued = queue.offer(task);
                    if (!requeued) {
                        log.error("Retry queue full, dropping message ackId={} subject={} payload={}",
                                task.ackId, task.subject, task.payload);
                    }
                    sleepQuietly(retryDelayMs);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("BillingRetryWorker interrupted — stopping. Undelivered tasks: {}", queue.size());
                break;

            } catch (Exception e) {
                log.error("Unexpected error in BillingRetryWorker loop", e);
                sleepQuietly(retryDelayMs);
            }
        }

        log.debug("BillingRetryWorker loop exited. Undelivered tasks: {}", queue.size());
    }

    private boolean attempt(RetryTask task) {
        try {
            return natsPublisherHelper.publish(task.subject, task.payload, task.ackId);
        } catch (Exception e) {
            log.error("BILLING retry attempt threw exception ackId={} subject={} payload={}",
                    task.ackId, task.subject, task.payload, e);
            return false;
        }
    }

    public int pendingCount() {
        return queue.size();
    }

    private void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Immutable snapshot of everything needed to re-attempt a billing NATS publish.
     * Stores the already-resolved subject and payload — no cache re-lookup on retry.
     */
    static final class RetryTask {

        final String subject;
        final GriffinUploaderNATSResponseModel payload;
        final String ackId;

        RetryTask(String subject,
                  GriffinUploaderNATSResponseModel payload,
                  String ackId) {
            this.subject = subject;
            this.payload = payload;
            this.ackId   = ackId;
        }
    }
}