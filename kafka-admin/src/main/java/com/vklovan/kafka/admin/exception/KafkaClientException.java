package com.vklovan.kafka.admin.exception;

public class KafkaClientException extends RuntimeException {

    public KafkaClientException(String msg) {
        super(msg);
    }

    public KafkaClientException(String msg, Throwable e) {
        super(msg, e);
    }
}
