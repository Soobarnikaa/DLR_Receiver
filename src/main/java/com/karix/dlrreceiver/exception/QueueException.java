package com.karix.dlrreceiver.exception;

public class QueueException extends RuntimeException {

    private static final String ERROR_CODE = "QUEUE_ERROR";

    public QueueException(String message) {
        super(message);
    }

    public QueueException(String message, Throwable cause) {
        super(message, cause);
    }

    public String getErrorCode() {
        return ERROR_CODE;
    }
}