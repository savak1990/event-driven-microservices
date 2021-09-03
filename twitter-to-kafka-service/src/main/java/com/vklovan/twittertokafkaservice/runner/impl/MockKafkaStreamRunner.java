package com.vklovan.twittertokafkaservice.runner.impl;

import com.github.javafaker.Faker;
import com.vklovan.appconfigdata.config.TwitterToKafkaServiceConfigData;
import com.vklovan.twittertokafkaservice.listener.TwitterKafkaStatusListener;
import com.vklovan.twittertokafkaservice.runner.StreamRunner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

@Service
@Slf4j
@RequiredArgsConstructor
@ConditionalOnProperty(
        name = "twitter-to-kafka-service.enable-mock-tweets",
        havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

    private final TwitterToKafkaServiceConfigData twitterConfigData;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;
    private final ThreadPoolTaskScheduler scheduler;

    private final Faker faker = Faker.instance();

    private static final String TWEET_AS_RAW_JSON_TEMPLATE = "{" +
            "\"created_at\":\"%s\"," +
            "\"id\":\"%s\"," +
            "\"text\":\"%s\"," +
            "\"user\":{\"id\":\"%s\"}" +
            "}";

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM d HH:mm:ss z yyyy";

    @Override
    public void start() {
        log.info("Started filtering twitter stream for keywords {}",
                Arrays.toString(twitterConfigData.getTwitterKeywords().toArray(new String[0])));
        long sleepTimeMs = twitterConfigData.getMockSleepMs();
        scheduler.scheduleAtFixedRate(this::tweet, Duration.of(sleepTimeMs, ChronoUnit.MILLIS));
    }

    private void tweet() {
        var keywords = twitterConfigData.getTwitterKeywords();
        String jsonTweet = getFormattedTweet(keywords);
        Status status;

        try {
            status = TwitterObjectFactory.createStatus(jsonTweet);
            twitterKafkaStatusListener.onStatus(status);
        } catch (TwitterException e) {
            e.printStackTrace();
            log.error("Error creating twitter status");
        }
    }

    private String getFormattedTweet(List<String> keywords) {
        String createdAt = ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH));
        String id = String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE));
        String text = getRandomTweetContent(keywords);
        String userId = String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE));
        return String.format(TWEET_AS_RAW_JSON_TEMPLATE, createdAt, id, text, userId);
    }

    private String getRandomTweetContent(List<String> keywords) {
        String quote = faker.harryPotter().quote();
        String keyword = keywords.get(ThreadLocalRandom.current().nextInt(keywords.size()));
        return String.format("%s: %s", keyword, quote).trim();
    }
}
