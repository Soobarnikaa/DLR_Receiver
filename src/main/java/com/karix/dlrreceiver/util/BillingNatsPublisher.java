package com.karix.dlrreceiver.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.karix.commonutil.enums.Category;
import com.karix.commonutil.enums.Channel;
import com.karix.commonutil.enums.DlrEventType;
import com.karix.commonutil.model.EmailDnRequest;
import com.karix.commonutil.model.EmailSubRequest;
import com.karix.commonutil.model.GriffinUploaderNATSResponseModel;
import com.karix.commonutil.model.SMSDNRequest;
import com.karix.commonutil.model.SMSSubmissionRequest;
import com.karix.dlrreceiver.service.RedisService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static com.karix.commonutil.nats.SubjectResolver.dlrBill;

@Component
public class BillingNatsPublisher {

    private static final Logger log = LogManager.getLogger(BillingNatsPublisher.class);

    private final ObjectMapper mapper;
    private final NatsPublisherHelper natsPublisherHelper;
    private final RedisService redisService;
    private final JsonUtil jsonUtil;

    @Value("${karix.nats.stream}")
    private String stream;

    public BillingNatsPublisher(ObjectMapper mapper,
                                NatsPublisherHelper natsPublisherHelper,
                                RedisService redisService,
                                JsonUtil jsonUtil) {
        this.jsonUtil = jsonUtil;
        this.mapper = mapper;
        this.natsPublisherHelper = natsPublisherHelper;
        this.redisService = redisService;
    }

    public PublishResult publish(Channel channel,
                                 Object request,
                                 String ackId,
                                 String mid,
                                 String clientId,
                                 Category categoryEnum,
                                 DlrEventType eventType,
                                 boolean platformRejected) {

        String subject = stream + "." + dlrBill(channel, categoryEnum);

        try {
            // platform rejected → direct publish (skip Redis)
            if (platformRejected) {

                GriffinUploaderNATSResponseModel payload =
                        buildSinglePayload(channel, request, ackId, mid, clientId, categoryEnum, eventType);

//                payload.setFailure(709, "Rejected due to platform rejection");

                boolean status = natsPublisherHelper.publish(subject, payload, ackId);

                if (!status) {
                    log.error("BILLING FAST-PATH publish failed ackId={} subject={}", ackId, subject);
                    return PublishResult.fail(subject, payload);
                }

                log.debug("BILLING FAST-PATH (platformRejected) published ackId={}", ackId);
                return PublishResult.ok();
            }

            GriffinUploaderNATSResponseModel payload = null;

            String subPayloadJson = redisService.getSubUploadPayload(channel, ackId, mid);
            String dnPayloadJson  = redisService.getDnUploadPayload(channel, ackId, mid);

            log.debug("handleEvent ~ Channel : {} ackId : {} mid : {} subPayloadJson = {}, dnPayloadJson = {}",
                    channel, ackId, mid, subPayloadJson, dnPayloadJson);

            // SUB FLOW
            if (eventType == DlrEventType.SUB) {

                if (dnPayloadJson != null) {
                    Object dnRequest = parseDnPayload(channel, dnPayloadJson);

                    payload = buildCombinedPayload(
                            channel,
                            request,
                            dnRequest,
                            ackId,
                            mid,
                            clientId,
                            categoryEnum
                    );

                    redisService.deleteDnUploadPayload(channel, categoryEnum, ackId, mid, clientId);
                    log.debug("DN came first - Combined SUB + DN for ackId={} mid={}", ackId, mid);

                } else {
                    redisService.setSubUploadPayload(channel, categoryEnum, ackId, mid,
                            jsonUtil.toJson(request), clientId);

                    log.debug("Stored SUB event in Redis ackId={} mid={}", ackId, mid);
                    return PublishResult.ok();
                }
            }

            // DN FLOW
            if (eventType == DlrEventType.DN) {

                if (subPayloadJson != null) {
                    Object subRequest = parseSubPayload(channel, subPayloadJson);

                    payload = buildCombinedPayload(
                            channel,
                            subRequest,
                            request,
                            ackId,
                            mid,
                            clientId,
                            categoryEnum
                    );

                    redisService.deleteSubUploadPayload(channel, categoryEnum, ackId, mid, clientId);
                    log.debug("SUB came first - Combined SUB + DN for ackId={} mid={}", ackId, mid);

                } else {
                    redisService.setDnUploadPayload(channel, categoryEnum, ackId, mid,
                            jsonUtil.toJson(request), clientId);

                    log.debug("Stored DN event in Redis ackId={} mid={}", ackId, mid);
                    return PublishResult.ok();
                }
            }

            // FINAL PUBLISH (only when payload is ready)
            if (payload == null) {
                return PublishResult.ok();
            }

            boolean status = natsPublisherHelper.publish(subject, payload, ackId);

            if (!status) {
                log.error("BILLING {} {} NATS publish returned false ackId={} subject={}",
                        channel, eventType, ackId, subject);
                return PublishResult.fail(subject, payload);
            }

            log.debug("BILLING {} {} published successfully subject={} ackId={}",
                    channel, eventType, subject, ackId);

            return PublishResult.ok();

        } catch (Exception e) {
            log.error("BILLING {} {} publish threw exception ackId={} subject={}",
                    channel, eventType, ackId, subject, e);
            return PublishResult.fail(subject, null);
        }
    }

    private Object parseSubPayload(Channel channel, String json) {
        try {
            if (channel == Channel.SMS) {
                return jsonUtil.fromJson(json, SMSSubmissionRequest.class);
            } else {
                return jsonUtil.fromJson(json, EmailSubRequest.class);
            }
        } catch (Exception e) {
            log.error("Failed to parse SUB payload from Redis", e);
            throw new RuntimeException(e);
        }
    }

    private Object parseDnPayload(Channel channel, String json) {
        try {
            if (channel == Channel.SMS) {
                return jsonUtil.fromJson(json, SMSDNRequest.class);
            } else {
                return jsonUtil.fromJson(json, EmailDnRequest.class);
            }
        } catch (Exception e) {
            log.error("Failed to parse DN payload from Redis", e);
            throw new RuntimeException(e);
        }
    }

    private GriffinUploaderNATSResponseModel buildCombinedPayload(
            Channel channel,
            Object subRequest,
            Object dnRequest,
            String ackId,
            String mid,
            String clientId,
            Category categoryEnum) {

        GriffinUploaderNATSResponseModel data = new GriffinUploaderNATSResponseModel();

        data.setAckId(ackId);
        data.setMID(mid);
        data.setChannel(channel);
        data.setCategory(categoryEnum);
        data.setClientId(clientId);

        if (channel == Channel.SMS) {

            if (subRequest instanceof SMSSubmissionRequest smsSub) {
                data.setSmsSubPayload(smsSub);
            }

            if (dnRequest instanceof SMSDNRequest smsDn) {
                data.setSmsDnPayload(smsDn);
            }

        } else {

            if (subRequest instanceof EmailSubRequest sub) {
                EmailSubRequest sanitized = mapper.convertValue(sub, EmailSubRequest.class);
                sanitized.setFilename(null);
                sanitized.setFile_content(null);
                data.setEmailSubPayload(sanitized);
            }

            if (dnRequest instanceof EmailDnRequest dn) {
                data.setEmailDnPayload(dn);
            }
        }

        return data;
    }

    private GriffinUploaderNATSResponseModel buildSinglePayload(
            Channel channel,
            Object request,
            String ackId,
            String mid,
            String clientId,
            Category categoryEnum,
            DlrEventType eventType) {

        GriffinUploaderNATSResponseModel data = new GriffinUploaderNATSResponseModel();

        data.setAckId(ackId);
        data.setMID(mid);
        data.setChannel(channel);
        data.setCategory(categoryEnum);
        data.setClientId(clientId);

        if (channel == Channel.SMS) {
            if (eventType == DlrEventType.SUB && request instanceof SMSSubmissionRequest smsSub) {
                data.setSmsSubPayload(smsSub);
            } else if (eventType == DlrEventType.DN && request instanceof SMSDNRequest smsDn) {
                data.setSmsDnPayload(smsDn);
            }
        } else {
            if (eventType == DlrEventType.SUB && request instanceof EmailSubRequest subRequest) {
                EmailSubRequest sanitized = mapper.convertValue(subRequest, EmailSubRequest.class);
                sanitized.setFilename(null);
                sanitized.setFile_content(null);
                data.setEmailSubPayload(sanitized);
            } else if (eventType == DlrEventType.DN && request instanceof EmailDnRequest dnRequest) {
                data.setEmailDnPayload(dnRequest);
            }
        }

        return data;
    }

    public static final class PublishResult {

        private final boolean success;
        private final String subject;
        private final GriffinUploaderNATSResponseModel payload;

        private PublishResult(boolean success,
                              String subject,
                              GriffinUploaderNATSResponseModel payload) {
            this.success = success;
            this.subject = subject;
            this.payload = payload;
        }

        public static PublishResult ok() {
            return new PublishResult(true, null, null);
        }

        public static PublishResult fail(String subject,
                                         GriffinUploaderNATSResponseModel payload) {
            return new PublishResult(false, subject, payload);
        }

        public boolean isSuccess() { return success; }

        public String subject() { return subject; }

        public GriffinUploaderNATSResponseModel payload() { return payload; }
    }
}