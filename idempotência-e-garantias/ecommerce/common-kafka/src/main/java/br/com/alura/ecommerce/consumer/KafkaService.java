package br.com.alura.ecommerce.consumer;

import br.com.alura.ecommerce.Message;
import br.com.alura.ecommerce.dispatcher.GsonSerializer;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
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

    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction<T> parse;

    private KafkaService(String groupName, ConsumerFunction<T> parse, Map<String,String> overrideProperties) {
        this.consumer = new KafkaConsumer<>(properties(groupName, overrideProperties));
        this.parse = parse;
    }

    public KafkaService(String groupName, String topic, ConsumerFunction<T> parse, Map<String,String> overrideProperties) {
        this(groupName, parse, overrideProperties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupName, Pattern pattern, ConsumerFunction<T> parse, Map<String,String> overrideProperties) {
        this(groupName, parse, overrideProperties);
        consumer.subscribe(pattern);
    }

    public void run() throws ExecutionException, InterruptedException {
        try(var deadLetterDispatcher = new KafkaDispatcher<>()) {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    System.out.println(records.count() + " record(s) found.");

                    for (var record : records) {
                        try {
                            parse.consume(record);
                        } catch (Exception e) {
                            e.printStackTrace();

                            var message = record.value();
                            deadLetterDispatcher.send(
                                    "ECOMMERCE_DEADLETTER",
                                    message.getId().toString(),
                                    message.getId().continueWith("DeadLetter"),
                                    new GsonSerializer().serialize("", message)
                            );

                            // se houver um erro no envio para a deadletter, estamos optando por matar o programa
                        }
                    }
                }
            }
        }
    }

    private Properties properties(String groupName, Map<String, String> overrideProperties) {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        // a declaração do grupo é fundamental para iniciar o consumidor
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupName);

        // podemos passar um id para o consumidor
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, String.format("%s-%s", groupName, UUID.randomUUID().toString()));

        // essa configuração define o comportamento do consumidor quando não há um offset inicial ou se o offset atual
        // não existe mais no servidor (esses cenários ocorrem geralmente quando surge um novo Consumer Group ou quando
        // um servidor ficou tanto tempo offline que, ao voltar, as mensagens que ele deveria ler já foram apagadas pela
        // política de retenção do Kafka). Os valores para essa config podem ser:
        // - earliest: lê todas as mensagens que estão disponíveis no tópico, desde a mais antiga.
        // - latest: ignora as mensagens antigas e lê somente as mensagens que chegarem após o momento que o Consumer
        //           Group iniciou.
        // - none: se não houver um offset gravado para o Consumer Group, o Kafka retornará um erro para o Consumidor no
        //         no momento do poll, ao invés de tentar resetar o offset para início ou fim.
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // sobrescreves propriedades passadas como parâmetro
        properties.putAll(overrideProperties);

        return properties;
    }

    @Override
    public void close() {
        this.consumer.close();
    }
}
