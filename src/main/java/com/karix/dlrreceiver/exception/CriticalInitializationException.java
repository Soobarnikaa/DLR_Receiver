package com.karix.dlrreceiver.exception;

public class CriticalInitializationException extends RuntimeException {

    public CriticalInitializationException(String message) {
        super(message);
    }

    public CriticalInitializationException(String message, Throwable cause) {
        super(message, cause);
    }

}