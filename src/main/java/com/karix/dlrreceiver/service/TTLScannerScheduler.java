package com.karix.dlrreceiver.service;

import com.karix.commonutil.enums.Category;
import com.karix.commonutil.enums.Channel;
import com.karix.commonutil.interfaces.DeliveryInterface;
import com.karix.commonutil.interfaces.SubmissionInterface;
import com.karix.commonutil.model.*;
import com.karix.commonutil.nats.SubjectResolver;
import com.karix.dlrreceiver.util.JsonUtil;
import com.karix.nats.core.producer.ProducerManager;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Set;

@Component
public class TTLScannerScheduler {

    private static final Logger log = LoggerFactory.getLogger(TTLScannerScheduler.class);

    private final RedisService redisService;
    private final JsonUtil jsonUtil;
    private final ProducerManager producerManager;

    public TTLScannerScheduler(RedisService redisService,
                               JsonUtil jsonUtil,
                               ProducerManager producerManager) {
        this.redisService = redisService;
        this.jsonUtil = jsonUtil;
        this.producerManager = producerManager;
    }

    @Value("${karix.nats.stream}")
    private String stream;

    @Scheduled(fixedDelayString = "${aggregation.sub.ttl.scanner.minutes:PT5M}")
    @SchedulerLock(name = "TTLScannerScheduler", lockAtMostFor = "5m", lockAtLeastFor = "30s")
    public void scan() {

        log.debug("Starting TTLScannerScheduler to scan for expired payloads.");

        Set<String> expiredKeys = redisService.fetchExpiredUploadPayloads();

        if (expiredKeys == null || expiredKeys.isEmpty()) {
            log.debug("No expired Sub Upload payloads found.");
            return;
        }

        for (String entry : expiredKeys) {
            try {
                log.debug("Processing entry = {}", entry);

                String[] parts = entry.split("\\|");
                if (parts.length != 6) {
                    log.warn("Malformed entry found, removing: {}", entry);
                    redisService.removeTTLForPayload(entry);
                    continue;
                }

                Channel channel = Channel.valueOf(parts[0]);
                Category category = Category.valueOf(parts[1]);
                String ackId = parts[2];
                String mid = parts[3];
                String type = parts[4];
                String clientId = parts[5];

                String subStatus = "";
                String dnStatus = "";
                if ("SUB".equalsIgnoreCase((type))) {
                     subStatus = redisService.getSubUploadPayload(channel, ackId, mid);
                }
                else {
                    dnStatus = redisService.getDnUploadPayload(channel, ackId, mid);
                }

                if (subStatus == null) {
                    log.error("No payload found for channel={}, category={}, ackId={}, mid={}",
                            channel, category, ackId, mid);
                    continue;
                }

                if (dnStatus == null) {
                    log.error("No payload found for channel={}, category={}, ackId={}, mid={}",
                            channel, category, ackId, mid);
                    continue;
                }

                SubmissionInterface subUploadStatus = null;
                DeliveryInterface dnUploadStatus = null;

                if (channel == Channel.SMS) {
                    if ("SUB".equalsIgnoreCase(type)) {
                        subUploadStatus = jsonUtil.fromJson(subStatus, SMSSubmissionRequest.class);
                    } else {
                        dnUploadStatus = jsonUtil.fromJson(dnStatus, SMSDNRequest.class);
                    }
                } else if (channel == Channel.EMAIL) {
                    if ("SUB".equalsIgnoreCase(type)) {
                        subUploadStatus = jsonUtil.fromJson(subStatus, EmailSubRequest.class);
                    } else {
                        dnUploadStatus = jsonUtil.fromJson(dnStatus, EmailDnRequest.class);
                    }
                }

                GriffinUploaderNATSResponseModel response = new GriffinUploaderNATSResponseModel();
                response.setChannel(channel);
                response.setCategory(category);
                response.setAckId(ackId);
                response.setMID(mid);

                response.setFailure(708,
                        "Submission TTL expired and DN or SUB not received.");

                if ("SUB".equalsIgnoreCase(type)) {
                    response.setSubPayload(subUploadStatus);
                } else {
                    response.setDnPayload(dnUploadStatus);
                }

                response.setClientId(clientId);
                publishResponse(response);

                if ("SUB".equalsIgnoreCase(type)) {
                    redisService.deleteSubUploadPayload(channel, category, ackId, mid, clientId);
                } else {
                    redisService.deleteDnUploadPayload(channel, category, ackId, mid, clientId);
                }
                redisService.removeTTLForPayload(entry);

            } catch (Exception e) {
                log.error("Error processing entry: {}", entry, e);
            }
        }
    }

    public void publishResponse(GriffinUploaderNATSResponseModel message) {
        try {
            log.debug("Publishing response to NATS");
            String subject = stream + "." + SubjectResolver.dlrBill(message.getChannel(), message.getCategory());
            Boolean status = producerManager.publishAsyncMessage(
                    subject,
                    jsonUtil.toJson(message),
                    List.of(1)
            );
            log.debug("Published response to NATS with subject={}, status={}", subject, status);
        } catch (Exception e) {
            log.error("Error publishing response", e);
        }
    }
}