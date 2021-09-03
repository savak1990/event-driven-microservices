package com.vklovan.twittertokafkaservice.init.impl;

import com.vklovan.appconfigdata.config.KafkaConfigData;
import com.vklovan.kafka.admin.client.KafkaAdminClient;
import com.vklovan.twittertokafkaservice.init.StreamInitializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class KafkaStreamInitializer implements StreamInitializer {

    private final KafkaConfigData kafkaConfigData;
    private final KafkaAdminClient kafkaAdminClient;

    @Override
    public void init() {
        kafkaAdminClient.createTopics();
        kafkaAdminClient.checkSchemaRegistry();
        log.info("Topics with name {} are ready for operations!",
                kafkaConfigData.getTopicNamesToCreate().toArray());
    }

}
