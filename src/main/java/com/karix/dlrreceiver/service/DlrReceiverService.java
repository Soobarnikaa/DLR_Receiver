package com.karix.dlrreceiver.service;

import com.karix.commonutil.model.EmailDnRequest;
import com.karix.commonutil.model.EmailSubRequest;
import com.karix.dlrreceiver.util.ApiValidations;
import org.springframework.stereotype.Service;
import com.karix.commonutil.model.SMSDNRequest;
import com.karix.commonutil.model.SMSSubmissionRequest;

@Service
public class DlrReceiverService {

    private final ApiValidations validations;

    public DlrReceiverService(ApiValidations validations) {
        this.validations = validations;
    }

    public void processSubmission(SMSSubmissionRequest request) {
        validations.smsSubProcess(request);
    }

    public void processDelivery(SMSDNRequest request) {
        validations.smsDnProcess(request);
    }

    public void processEmailSubmission(EmailSubRequest request) {
        validations.emailSubProcess(request);
    }

    public void processEmailDn(EmailDnRequest request) {
        validations.emailDnProcess(request);
    }
}
