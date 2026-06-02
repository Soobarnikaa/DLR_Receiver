package com.karix.dlrreceiver.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.karix.commonutil.model.*;
import com.karix.dlrreceiver.service.RedisQueueService;
import com.karix.dlrreceiver.service.DlrReceiverService;
import com.karix.dlrreceiver.util.EmailNatsPublisher;
import com.karix.dlrreceiver.util.SmsNatsPublisher;
import jakarta.validation.Valid;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping("/receiver")
public class DlrReceiverController {
    private static final Logger log = LogManager.getLogger(DlrReceiverController.class);

    private final DlrReceiverService service;
    private final SmsNatsPublisher smsNatsPublisher;
    private final EmailNatsPublisher emailNatsPublisher;
    private final RedisQueueService queueService;
    private final ObjectMapper mapper;

    public DlrReceiverController(DlrReceiverService service,
                                 SmsNatsPublisher smsNatsPublisher,
                                 EmailNatsPublisher emailNatsPublisher,
                                 RedisQueueService queueService,
                                 ObjectMapper mapper
                                 ) {
        this.service = service;
        this.smsNatsPublisher = smsNatsPublisher;
        this.emailNatsPublisher = emailNatsPublisher;
        this.queueService = queueService;
        this.mapper = mapper;
    }

    @PostMapping(value = "/subsms", consumes = "application/json")
    public ResponseEntity<DlrReceiverResponse> smsSub(@Valid @RequestBody SMSSubmissionRequest request) {

        log.info("SMS Submissions Processing");
        log.debug("SMS Submission request received ackId={}, request:{}", request.getFileid(), request);
        service.processSubmission(request);

        String ackId = request.getFileid();

        try {

            smsNatsPublisher.publishSmsSubmission(request, ackId);

            return ResponseEntity.ok(new DlrReceiverResponse(ackId));

        } catch (Exception e) {

            log.error("SMS SUBMISSION FAILED ackId={} request={} error={}",
                    ackId, request, e.getMessage());

            throw e;
        }
    }

    @PostMapping(value = "/dlrsms", consumes = "application/json")
    public ResponseEntity<DlrReceiverResponse> smsDn(@Valid @RequestBody SMSDNRequest request) {

        log.info("SMS DN Processing");
        log.debug("SMS DN request received ackId={}, request:{}", request.getFileid(), request);
        service.processDelivery(request);

        String ackId = request.getFileid();

        try {

            smsNatsPublisher.publishSmsDn(request, ackId);

            return ResponseEntity.ok(new DlrReceiverResponse(ackId));

        } catch (Exception e) {

            log.error("SMS DN FAILED ackId={} request={} error={}",
                    ackId, request, e.getMessage());

            throw e;
        }
    }


    @PostMapping(value = "/subemail", consumes = "application/json")
    public ResponseEntity<DlrReceiverResponse> emailSub(@Valid @RequestBody EmailSubRequest request) {

        log.info("EMAIL Submission request received ackId={}", request.getAckid());
        log.debug("EMAIL Submission request received ackId={}, request:{}", request.getAckid(), request);
        service.processEmailSubmission(request);

        String ackId = request.getAckid();

        String payload;
        try {
            payload = mapper.writeValueAsString(request);
        } catch (Exception e) {
            log.error("Serialization failed for ackId={}", ackId, e);
            throw new IllegalArgumentException("Invalid request payload", e);
        }

            if (request.getFilename() != null && !request.getFilename().isBlank() &&
                    request.getFile_content() != null && !request.getFile_content().isBlank()) {
            queueService.push(payload);
        }
        else {
            emailNatsPublisher.publishEmailSubmission(request,null,ackId);
        }
        return ResponseEntity.ok(new DlrReceiverResponse(ackId));
    }


    @PostMapping(value = "/dlremail", consumes = "application/json")
    public ResponseEntity<DlrReceiverResponse> emailDN(@Valid @RequestBody EmailDnRequest request) {

        log.info("EMAIL request processed");
        log.debug("EMAIL DN request received ackId={}, request:{}", request.getAckId(), request);
        service.processEmailDn(request);
        String ackId = request.getAckId();

    try{
        emailNatsPublisher.publishEmailDn(request, ackId);

        return ResponseEntity.ok(new DlrReceiverResponse(ackId));
    }catch (Exception e) {
        log.info("EMAIL DN FAILED ackId={} request={} error={}", ackId, request, e.getMessage());
        throw e;
    }
    }

}
