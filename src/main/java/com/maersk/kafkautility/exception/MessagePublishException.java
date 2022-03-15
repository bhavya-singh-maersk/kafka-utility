package com.maersk.kafkautility.exception;

public class MessagePublishException extends RuntimeException {

    private int retryCount;

    public int getRetryCount() {
        return retryCount;
    }

    public MessagePublishException(String message, int retryCount) {
        super(message);
        this.retryCount = retryCount;
    }

    public MessagePublishException(String message) {
        super(message);
    }
}
