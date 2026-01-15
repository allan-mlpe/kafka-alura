package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class KafkaService implements Closeable {

    private final KafkaConsumer<String, String> consumer;
    private final ConsumerFunction parse;

    public KafkaService(String groupName, String topic, ConsumerFunction parse) {
        this.consumer = new KafkaConsumer<>(properties(groupName));
        this.parse = parse;

        consumer.subscribe(Collections.singletonList(topic));
    }

    public void run() {
        while(true) {
            var records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println(records.count() + " record(s) found.");

                for (var record : records) {
                    parse.consume(record);
                }
            }
        }
    }

    private static Properties properties(String groupName) {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // a declaração do grupo é fundamental para iniciar o consumidor
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupName);

        // podemos passar um id para o consumidor
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, String.format("%s-%s", groupName, UUID.randomUUID().toString()));

        return properties;
    }

    @Override
    public void close() {
        this.consumer.close();
    }
}
