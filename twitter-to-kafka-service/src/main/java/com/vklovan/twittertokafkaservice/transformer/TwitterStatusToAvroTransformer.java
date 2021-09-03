package com.vklovan.twittertokafkaservice.transformer;

import com.vklovan.kafkamodel.avro.model.TwitterAvroModel;
import org.springframework.stereotype.Component;
import twitter4j.Status;

import java.util.Objects;

@Component
public class TwitterStatusToAvroTransformer {

    public TwitterAvroModel getTwitterAvroModelFromStatus(Status status) {
        return TwitterAvroModel.newBuilder()
                .setId(status.getId())
                .setUserId(status.getUser().getId())
                .setText(status.getText())
                .setCreatedAt(status.getCreatedAt() != null
                        ? status.getCreatedAt().getTime()
                        : null)
                .build();
    }
}
