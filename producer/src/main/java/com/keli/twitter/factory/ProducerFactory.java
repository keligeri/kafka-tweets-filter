package com.keli.twitter.factory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerFactory {


    public static KafkaProducer<String, String> createProducer(String bootstrapServer) {
        Properties properties = getProperties(bootstrapServer);
        return new KafkaProducer<>(properties);
    }

    public static KafkaProducer<String, String> createIdempotenceProducer(String bootstrapServer) {
        Properties properties = getIdempotenceProperties(bootstrapServer);
        return new KafkaProducer<>(properties);
    }

    public static KafkaProducer<String, String> createHighThroughputIdempotenceProducer(
            String bootstrapServer) {

        Properties properties = getIdempotenceProperties(bootstrapServer);
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");

        String thirtyTwoKb = Integer.toString(32 * 1024);
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, thirtyTwoKb);

        return new KafkaProducer<>(properties);
    }

    private static Properties getIdempotenceProperties(String bootstrapServer) {
        Properties properties = getProperties(bootstrapServer);
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        return properties;
    }

    private static Properties getProperties(String bootstrapServer) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
