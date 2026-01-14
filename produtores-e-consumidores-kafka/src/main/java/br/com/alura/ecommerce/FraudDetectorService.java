package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class FraudDetectorService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());

        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));

        while(true) {
            var records = consumer.poll(Duration.ofMillis(100)); // esses 100ms são uma espécie de timeout
                                                                 // que esperaremos até que existam mensagens no tópico
                                                                 // até seguir a execução do código.

            if (!records.isEmpty()) {
                System.out.println(records.count() + " record(s) found.");

                for (var record : records) {
                    System.out.println("------------------------------------------------");
                    System.out.println("Processing new order, checking for fraud...");
                    System.out.println("- Key::" + record.key());
                    System.out.println("- Value::" + record.value());
                    System.out.println("- Partition::" + record.partition());
                    System.out.println("- Offset::" + record.offset());
                    System.out.println("- Topic::" + record.topic());

                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        // ignoring
                        e.printStackTrace();
                    }

                    System.out.println("Order processed!");
                }
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // a declaração do grupo é fundamental para iniciar o consumidor
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());

        // podemos passar um id para o consumidor
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, String.format("%s-%s", FraudDetectorService.class.getSimpleName(), UUID.randomUUID().toString()));

        // a config max.poll.records diz para o kafka quantas mensagens enviar a cada poll

        // ela tem relação com o tempo de processamento (max.poll.interval.ms)
        // que é o tempo que devemos terminar de processar as mensagens buscadas em cada poll.
        //
        // caso contrário, o kafka entenderá que o consumidor "travou",
        // expulsará ele do grupo e iniciará um rebalanceamento.
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");


        return properties;
    }
}
