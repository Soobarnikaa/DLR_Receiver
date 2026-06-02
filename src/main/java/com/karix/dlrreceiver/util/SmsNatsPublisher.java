package com.karix.dlrreceiver.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.karix.commonutil.cache.CacheInterface;
import com.karix.commonutil.enums.Category;
import com.karix.commonutil.enums.Channel;
import com.karix.commonutil.enums.DlrEventType;
import com.karix.commonutil.model.DnAggregatorRequest;
import com.karix.commonutil.model.SMSDNRequest;
import com.karix.commonutil.model.SMSSubmissionRequest;
import com.karix.commonutil.utils.ValidationConstants;
import com.karix.dlrreceiver.bootstrap.BillingRetryWorker;
import com.karix.dlrreceiver.spillover.PluginApiSpilloverService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class SmsNatsPublisher {

    private static final Logger log = LogManager.getLogger(SmsNatsPublisher.class);

    private final Environment env;
    private final PluginApiSpilloverService spilloverService;
    private final CacheInterface cacheWrapper;
    private final ObjectMapper mapper;
    private final NatsPublisherHelper natsPublisherHelper;
    private final FlowRouter flowRouter;
    private final BillingNatsPublisher billingNatsPublisher;
    private final BillingRetryWorker billingRetryWorker;

    @Value("${karix.nats.stream}")
    private String stream;

    @Value("${sms.dn.rejected.status}")
    private String rejected;

    public SmsNatsPublisher(Environment env,
                            PluginApiSpilloverService spilloverService,
                            @Qualifier("cacheWrapper") CacheInterface cacheWrapper,
                            ObjectMapper mapper,
                            NatsPublisherHelper natsPublisherHelper,
                            FlowRouter flowRouter,
                            BillingNatsPublisher billingNatsPublisher,
                            BillingRetryWorker billingRetryWorker) {
        this.env = env;
        this.spilloverService = spilloverService;
        this.cacheWrapper = cacheWrapper;
        this.mapper = mapper;
        this.natsPublisherHelper = natsPublisherHelper;
        this.flowRouter = flowRouter;
        this.billingNatsPublisher = billingNatsPublisher;
        this.billingRetryWorker = billingRetryWorker;
    }

    public void publishSmsSubmission(SMSSubmissionRequest request, String ackId) {
        publish(request, ackId, request.getMid(), request.getEsmeaddr(), DlrEventType.SUB);
    }

    public void publishSmsDn(SMSDNRequest request, String ackId) {
        publish(request, ackId, request.getMid(), request.getEsmeaddr(), DlrEventType.DN);
    }

    private void publish(Object request,
                         String ackId,
                         String mid,
                         String esmeAddr,
                         DlrEventType eventType
) {

        String rawCategory = cacheWrapper.getAccConfig(esmeAddr);
        log.debug("SMS category from cache={} ackId={}", rawCategory, ackId);

        if (rawCategory == null || rawCategory.isBlank() || rawCategory.equalsIgnoreCase("default")) {
            rawCategory = env.getProperty("category.blank", "UNKNOWN");
        }

        Category categoryEnum = Category.valueOf(rawCategory.toUpperCase());
        String categoryStr = env.getProperty("category." + rawCategory);
        log.debug("SMS category from env={} ackId={}", categoryStr, ackId);

        String clientId = cacheWrapper.getClientIdInfo(esmeAddr, ValidationConstants.SMS_CHANNEL, categoryStr);
        log.debug("SMS clientId={} ackId={}", clientId, ackId);

        FlowDecision decision = resolveFlow(request, eventType, ackId);
        FlowRouter.Flow flow = decision.getFlow();
        log.debug("SMS {} ackId={} → {} flow", eventType, ackId, flow);

        if (flow == FlowRouter.Flow.BILLING) {
            BillingNatsPublisher.PublishResult result =
                    billingNatsPublisher.publish(Channel.SMS, request, ackId, mid, clientId, categoryEnum, eventType,decision.isPlatformRejected());

            if (!result.isSuccess()) {
                billingRetryWorker.enqueue(result.subject(), result.payload(), ackId);
            }
            return;
        }

        DnAggregatorRequest data = new DnAggregatorRequest();
        data.setEventType(eventType);
        data.setAckId(ackId);
        data.setChannel(Channel.SMS);
        data.setCategory(categoryEnum);
        data.setMid(mid);

        if (request instanceof SMSDNRequest smsDn) {
            data.setSmsDnPayload(smsDn);
        } else if (request instanceof SMSSubmissionRequest smsSub) {
            data.setSmsSubPayload(smsSub);
        }

        try {
            String subject = buildSubject(categoryEnum.name(), eventType);

            ObjectNode node = mapper.valueToTree(data);
            node.remove("subPayload");
            node.remove("dnpayload");

            boolean status = natsPublisherHelper.publish(subject, node, ackId);

            if (!status) {
                log.error("GRIFFIN SMS NATS publish returned false ackId={}", ackId);
                insertIntoSpillover(data, mid, clientId, ackId,
                        ValidationConstants.SMS_CHANNEL, categoryStr, eventType);
                return;
            }

            log.debug("GRIFFIN SMS {} published successfully subject={} ackId={}", eventType, subject, ackId);

        } catch (Exception e) {
            log.error("GRIFFIN SMS {} publish failed ackId={}", eventType, ackId, e);
            insertIntoSpillover(data, mid, clientId, ackId,
                    ValidationConstants.SMS_CHANNEL, categoryStr, eventType);
        }
    }

    private String buildSubject(String category, DlrEventType eventType) {
        String suffix = DlrEventType.DN.equals(eventType) ? "_DLR_Q" : "_SUB_Q";
        return stream + "." + ValidationConstants.SMS_CHANNEL + "_" + category + suffix;
    }

    private void insertIntoSpillover(Object data,
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
                    mapper.writeValueAsString(data),
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

        if (eventType == DlrEventType.DN && request instanceof SMSDNRequest smsDn) {

            if (isBothRejected(smsDn)) {

                log.debug("SMS DN both statuses REJECTED, forcing BILLING flow ackId={} " +
                                "platform_status={} carrier_operator_status={}",
                        ackId,
                        smsDn.getPlatformStatus(),
                        smsDn.getCarrierOperatorStatus());

                return new FlowDecision(FlowRouter.Flow.BILLING, true);
            }
        }

        FlowRouter.Flow flow = flowRouter.route(Channel.SMS, ackId);

        return new FlowDecision(flow, false);
    }

    private boolean isBothRejected(SMSDNRequest smsDn) {
        return rejected.equalsIgnoreCase(smsDn.getPlatformStatus())
                && rejected.equalsIgnoreCase(smsDn.getCarrierOperatorStatus());
    }
}