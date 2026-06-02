package com.karix.dlrreceiver.bootstrap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.karix.commonutil.model.EmailSubRequest;
import com.karix.dlrreceiver.service.RedisQueueService;
import com.karix.dlrreceiver.util.ApiValidations;
import com.karix.dlrreceiver.util.EmailNatsPublisher;
import jakarta.annotation.PostConstruct;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class DlrWorker {

    private static final Logger log = LogManager.getLogger(DlrWorker.class);

    private final RedisQueueService queueService;
    private final ObjectMapper mapper;
    private final ApiValidations apiValidations;
    private final EmailNatsPublisher emailNatsPublisher;
    private ExecutorService executorService;

    @Value("${worker.thread.count:5}")
    private int workerThreadCount;

    @Value("${worker.redis.poll.timeout.ms:5000}")
    private long redisErrorBackoffMs;


    private volatile boolean running = true;

    public DlrWorker(RedisQueueService queueService,
                     ObjectMapper mapper,
                     ApiValidations apiValidations,
                     EmailNatsPublisher emailNatsPublisher) {

        this.queueService = queueService;
        this.mapper = mapper;
        this.apiValidations = apiValidations;
        this.emailNatsPublisher = emailNatsPublisher;
    }

    @PostConstruct
    public void startWorkers() {

        if (workerThreadCount < 1) {
            throw new IllegalStateException("worker.thread.count must be >= 1");
        }

        this.executorService = Executors.newFixedThreadPool(
                workerThreadCount,
                namedThreadFactory()
        );

        for (int i = 0; i < workerThreadCount; i++) {
            executorService.submit(this::processLoop);
        }

        log.info("Started {} DLR worker threads", workerThreadCount);
    }

    private void processLoop() {

        while (running) {

            String payload = null;

            try {
                payload = queueService.take();

                if (payload != null) {
                    EmailSubRequest req = mapper.readValue(payload, EmailSubRequest.class);
                    process(req);
                }

            } catch (JsonProcessingException e) {
                log.error("Invalid JSON, discarding poison pill. payload={}", payload, e);

            } catch (InvalidDataAccessApiUsageException e) {
                log.error("Redis error, message may be lost. payload={} worker={}, backing off {}ms",
                        payload, Thread.currentThread().getName(), redisErrorBackoffMs, e);
                sleepQuietly(redisErrorBackoffMs);

            } catch (Exception e) {
                log.error("Worker processing failed, message may be lost. payload={} thread={}",
                        payload, Thread.currentThread().getName(), e);
            }
        }

        log.info("Worker stopped thread={}", Thread.currentThread().getName());
    }

    public void process(EmailSubRequest req) {
        try {
            Path path = apiValidations.writeToNfs(req);

            if (path == null) {
                log.warn("File write skipped for ackId={}", req.getAckid());
                return;
            }

            emailNatsPublisher.publishEmailSubmission(req, path, req.getAckid());

            log.debug("Processed ackId={}", req.getAckid());

        } catch (Exception e) {
            log.error("Processing failed for ackId={}", req.getAckid(), e);
        }
    }

    private ThreadFactory namedThreadFactory() {
        AtomicInteger counter = new AtomicInteger(1);

        return runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("dlr-worker-" + counter.getAndIncrement());
            thread.setDaemon(false);
            return thread;
        };
    }

    private void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    public void stop() {

        log.info("Stopping DLR workers...");

        running = false;

        if (executorService != null) {
            executorService.shutdown();

            try {
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.warn("Forcing shutdown of DLR workers...");
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.warn("Shutdown interrupted, forcing now...");
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        log.info("DLR workers stopped");
    }
}