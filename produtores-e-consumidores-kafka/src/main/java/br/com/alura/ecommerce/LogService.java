package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        var logService = new LogService();
        try (var service = new KafkaService(
                LogService.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
                logService::parse
        )) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println(String.format(
                "[LOG | %s | %s] key: %s, value: %s, partition: %s, offset: %s",
                NewOrderMain.class.getSimpleName(),
                record.topic(),
                record.key(),
                record.value(),
                record.partition(),
                record.offset()
        ));
    }
}
