package com.keli.twitter;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {

    private static final Logger logger  = LoggerFactory.getLogger(Producer.class);
    private static final String TWITTER_TOPIC = "twitter_topic";

    public void produce(String msg) {

        //producer.send(record, new TwitterCallback());
    }

    public static final class TwitterCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null) {
                String succeedMessage = new StringBuilder("Produced: ")
                        .append("Timestamp: ")
                        .append(metadata.timestamp())
                        .append(" - Partition ")
                        .append(metadata.partition())
                        .append(" - Offset ")
                        .append(metadata.offset()).toString();
                logger.info(succeedMessage);
            } else {
                logger.error("Exception: {}", exception);
            }
        }
    }
}
