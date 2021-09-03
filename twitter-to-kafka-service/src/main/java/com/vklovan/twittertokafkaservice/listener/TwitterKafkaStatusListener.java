package com.vklovan.twittertokafkaservice.listener;

import com.vklovan.appconfigdata.config.KafkaConfigData;
import com.vklovan.kafka.producer.service.KafkaProducer;
import com.vklovan.kafkamodel.avro.model.TwitterAvroModel;
import com.vklovan.twittertokafkaservice.transformer.TwitterStatusToAvroTransformer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
@Slf4j
@RequiredArgsConstructor
public class TwitterKafkaStatusListener extends StatusAdapter {

    private final KafkaConfigData kafkaConfigData;
    private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;
    private final TwitterStatusToAvroTransformer transformer;

    @Override
    public void onStatus(Status status) {
        log.info("Status: <{}>; sending to kafka topic {}",
                status, kafkaConfigData.getTopicName());

        TwitterAvroModel twitterAvroModel = transformer
                .getTwitterAvroModelFromStatus(status);

        kafkaProducer.send(kafkaConfigData.getTopicName(),
                twitterAvroModel.getUserId(), twitterAvroModel);
    }
}
