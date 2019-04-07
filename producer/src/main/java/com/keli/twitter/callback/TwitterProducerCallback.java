package com.keli.twitter.callback;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.logging.Logger;

public class TwitterProducerCallback implements Callback {

    private static final Logger logger = Logger.getLogger(TwitterProducerCallback.class.getName());

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
            logger.warning("Exception: " + exception);
        }
    }
}
