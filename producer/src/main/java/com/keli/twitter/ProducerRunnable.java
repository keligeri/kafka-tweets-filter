package com.keli.twitter;

import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ProducerRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(App.class.getName());

    private final KafkaProducer<String, String> producer;
    private final Client twitterClient;
    private final Callback callback;
    private final String topic;

    private final BlockingQueue<String> queue;

    public ProducerRunnable(
            KafkaProducer<String, String> producer, Client twitterClient,
            BlockingQueue<String> queue, String topic, Callback callback) {

        this.producer = producer;
        this.twitterClient = twitterClient;
        this.topic = topic;
        this.callback = callback;

        this.queue = queue;
    }

    @Override
    public void run() {
        twitterClient.connect();
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = queue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }

            if (msg != null) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
                producer.send(record, callback);
            }
        }
    }
}
