package com.example.iggy.flink.client;

/**
 * Exception thrown when Iggy client operations fail.
 */
public class IggyClientException extends RuntimeException {

    public IggyClientException(String message) {
        super(message);
    }

    public IggyClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
