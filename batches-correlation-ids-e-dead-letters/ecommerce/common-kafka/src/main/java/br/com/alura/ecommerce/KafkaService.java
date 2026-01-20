package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction<T> parse;

    private KafkaService(String groupName, ConsumerFunction<T> parse, Class<T> type, Map<String,String> overrideProperties) {
        this.consumer = new KafkaConsumer<>(properties(groupName, overrideProperties, type));
        this.parse = parse;
    }

    public KafkaService(String groupName, String topic, ConsumerFunction<T> parse, Class<T> type , Map<String,String> overrideProperties) {
        this(groupName, parse, type, overrideProperties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupName, Pattern pattern, ConsumerFunction<T> parse, Class<T> type, Map<String,String> overrideProperties) {
        this(groupName, parse, type, overrideProperties);
        consumer.subscribe(pattern);
    }

    public void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println(records.count() + " record(s) found.");

                for (var record : records) {
                    try {
                        parse.consume(record);
                    } catch (Exception e) { // only catches Exception because no matter which Exception
                                            // I want to recover and parse the next one

                        // just logging the exception message for now
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private Properties properties(String groupName, Map<String, String> overrideProperties, Class<T> type) {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        // a declaração do grupo é fundamental para iniciar o consumidor
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupName);

        // podemos passar um id para o consumidor
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, String.format("%s-%s", groupName, UUID.randomUUID().toString()));

        // sobrescreves propriedades passadas como parâmetro
        properties.putAll(overrideProperties);

        return properties;
    }

    @Override
    public void close() {
        this.consumer.close();
    }
}
