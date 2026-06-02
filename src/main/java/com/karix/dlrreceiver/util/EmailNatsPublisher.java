package com.karix.dlrreceiver.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.karix.commonutil.cache.CacheInterface;
import com.karix.commonutil.enums.Category;
import com.karix.commonutil.enums.Channel;
import com.karix.commonutil.enums.DlrEventType;
import com.karix.commonutil.model.DnAggregatorRequest;
import com.karix.commonutil.model.EmailDnRequest;
import com.karix.commonutil.model.EmailSubRequest;
import com.karix.commonutil.utils.ValidationConstants;
import com.karix.dlrreceiver.bootstrap.BillingRetryWorker;
import com.karix.dlrreceiver.spillover.PluginApiSpilloverService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import java.nio.file.Path;
import java.util.Set;

@Component
public class EmailNatsPublisher {

    private static final Logger log = LogManager.getLogger(EmailNatsPublisher.class);

    private final Environment env;
    private final CacheInterface cacheWrapper;
    private final PluginApiSpilloverService spilloverService;
    private final ObjectMapper mapper;
    private final NatsPublisherHelper natsPublisherHelper;
    private final FlowRouter flowRouter;
    private final BillingNatsPublisher billingNatsPublisher;
    private final BillingRetryWorker billingRetryWorker;

    @Value("${karix.nats.stream}")
    private String stream;

    @Value("${email.platform.rejection.event.codes}")
    private Set<String> rejectionEventCodes;

    public EmailNatsPublisher(Environment env,
                              @Qualifier("cacheWrapper") CacheInterface cacheWrapper,
                              PluginApiSpilloverService spilloverService,
                              ObjectMapper mapper,
                              NatsPublisherHelper natsPublisherHelper,
                              FlowRouter flowRouter,
                              BillingNatsPublisher billingNatsPublisher,
                              BillingRetryWorker billingRetryWorker) {
        this.env = env;
        this.cacheWrapper = cacheWrapper;
        this.spilloverService = spilloverService;
        this.mapper = mapper;
        this.natsPublisherHelper = natsPublisherHelper;
        this.flowRouter = flowRouter;
        this.billingNatsPublisher = billingNatsPublisher;
        this.billingRetryWorker = billingRetryWorker;
    }

    public void publishEmailSubmission(EmailSubRequest request, Path filepath, String ackId) {
        publish(request, filepath, ackId, request.getEsmeaddr(), request.getMid(), DlrEventType.SUB);
    }

    public void publishEmailDn(EmailDnRequest request, String ackId) {
        publish(request, null, ackId, request.getEsme(), request.getMid(), DlrEventType.DN);
    }

    private void publish(Object request,
                         Path filepath,
                         String ackId,
                         String esmeAddr,
                         String mid,
                         DlrEventType eventType) {

        String rawCategory = cacheWrapper.getAccConfig(esmeAddr);
        log.debug("EMAIL category from cache={} ackId={}", rawCategory, ackId);

        if (rawCategory == null || rawCategory.isBlank() || rawCategory.equalsIgnoreCase("default")) {
            rawCategory = env.getProperty("category.blank", "UNKNOWN");
        }

        Category categoryEnum = Category.valueOf(rawCategory.toUpperCase());
        String categoryStr = env.getProperty("category." + rawCategory);
        log.debug("EMAIL category from env={} ackId={}", categoryStr, ackId);

        String clientId = cacheWrapper.getClientIdInfo(esmeAddr, ValidationConstants.EMAIL_CHANNEL, categoryStr);
        log.debug("EMAIL clientId={} ackId={}", clientId, ackId);

        FlowDecision decision = resolveFlow(request, eventType, ackId);
        FlowRouter.Flow flow = decision.getFlow();
        log.debug("EMAIL {} ackId={} → {} flow and decision:{}", eventType, ackId, flow,decision.isPlatformRejected());

        if (flow == FlowRouter.Flow.BILLING) {
            BillingNatsPublisher.PublishResult result =
                    billingNatsPublisher.publish(Channel.EMAIL, request, ackId, mid, clientId, categoryEnum, eventType,decision.isPlatformRejected());

            if (!result.isSuccess()) {
                billingRetryWorker.enqueue(result.subject(), result.payload(), ackId);
            }
            return;
        }

        DnAggregatorRequest data = new DnAggregatorRequest();
        data.setEventType(eventType);
        data.setAckId(ackId);
        data.setChannel(Channel.EMAIL);
        data.setCategory(categoryEnum);
        data.setMid(mid);

        if (filepath != null) {
            data.setFilePath(filepath.toString());
        }

        if (request instanceof EmailSubRequest subRequest) {
            EmailSubRequest sanitized = mapper.convertValue(subRequest, EmailSubRequest.class);
            sanitized.setFilename(null);
            sanitized.setFile_content(null);
            data.setEmailSubPayload(sanitized);
        } else if (request instanceof EmailDnRequest dnRequest) {
            data.setEmailDnPayload(dnRequest);
        }

        try {
            String subject = buildSubject(categoryEnum.name(), eventType);

            ObjectNode node = mapper.valueToTree(data);
            node.remove("subPayload");
            node.remove("dnpayload");

            boolean status = natsPublisherHelper.publish(subject, node, ackId);

            if (!status) {
                log.error("GRIFFIN EMAIL NATS publish returned false ackId={}", ackId);
                insertIntoSpillover(request, mid, clientId, ackId,
                        ValidationConstants.EMAIL_CHANNEL, categoryStr, eventType);
                return;
            }

            log.debug("GRIFFIN EMAIL {} published successfully ackId={}", eventType, ackId);

        } catch (Exception e) {
            log.error("GRIFFIN EMAIL {} publish failed ackId={}", eventType, ackId, e);
            insertIntoSpillover(request, mid, clientId, ackId,
                    ValidationConstants.EMAIL_CHANNEL, categoryStr, eventType);
        }
    }

    private String buildSubject(String category, DlrEventType eventType) {
        String suffix = DlrEventType.DN.equals(eventType) ? "_DLR_Q" : "_SUB_Q";
        return stream + "." + ValidationConstants.EMAIL_CHANNEL + "_" + category + suffix;
    }

    private void insertIntoSpillover(Object request,
                                     String mid,
                                     String clientId,
                                     String ackId,
                                     String channel,
                                     String category,
                                     DlrEventType eventType) {
        try {
            String type = DlrEventType.DN.equals(eventType) ? DlrEventType.DN.name() : DlrEventType.SUB.name();
            spilloverService.insertSpillover(
                    ackId, mid, clientId, channel,
                    mapper.writeValueAsString(request),
                    Integer.parseInt(category),
                    type
            );
        } catch (Exception ex) {
            log.error("Spillover failed ackId={}", ackId, ex);
        }
    }

    private FlowDecision resolveFlow(Object request,
                                     DlrEventType eventType,
                                     String ackId) {

        if (eventType == DlrEventType.DN && request instanceof EmailDnRequest emailDn) {
            if (isRejected(emailDn)) {
                log.debug("EMAIL DN eventCode REJECTED, forcing BILLING flow ackId={} " +
                                "platform_status={}",
                        ackId,
                        emailDn.getStatusDesc());
                return new FlowDecision(FlowRouter.Flow.BILLING, true);
            }
        }

        FlowRouter.Flow flow = flowRouter.route(Channel.EMAIL, ackId);

        return new FlowDecision(flow, false);
    }

    private boolean isRejected(EmailDnRequest emailDnRequestDn) {
        String eventCode = emailDnRequestDn.getEventCode();
        if (eventCode == null) {
            return false;
        }

        return rejectionEventCodes.contains(eventCode.trim());
    }
}