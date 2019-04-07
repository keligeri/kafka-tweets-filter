package com.keli.twitter;

import com.keli.twitter.callback.TwitterProducerCallback;
import com.keli.twitter.factory.ProducerFactory;
import com.keli.twitter.factory.TwitterClientFactory;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

public class App {

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TWITTER_TOPIC = "twitter_topic";

    private static final String JAVA_TERM = "java";
    private static final String SPORT_TERM = "sport";
    private static final String SOCCER_TERM = "soccer";
    private static final String BITCOIN_TERM = "bitcoin";

    private static final Callback callback = new TwitterProducerCallback();

    public static void main(String[] args) {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(100000);
        List<String> termsToFollow = asList(JAVA_TERM, SPORT_TERM, SOCCER_TERM, BITCOIN_TERM);
        Client client = TwitterClientFactory.createTwitterClient(queue, termsToFollow);
        KafkaProducer<String, String> producer = ProducerFactory.createIdempotenceProducer(BOOTSTRAP_SERVER);

        client.connect();
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = queue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (msg != null) {
                ProducerRecord<String, String> record = new ProducerRecord<>(TWITTER_TOPIC, msg);
                producer.send(record, callback);
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.close();
            client.stop();

            System.out.println("Shutdown hook.......");
        }));
    }
}
