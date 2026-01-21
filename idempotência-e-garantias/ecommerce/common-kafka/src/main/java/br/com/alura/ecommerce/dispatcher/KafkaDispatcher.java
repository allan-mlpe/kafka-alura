package br.com.alura.ecommerce.dispatcher;

import br.com.alura.ecommerce.CorrelationId;
import br.com.alura.ecommerce.Message;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaDispatcher<T> implements Closeable {

    private final KafkaProducer<String, Message<T>> producer;

    public KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());
    }

    public void send(String topic, String key, CorrelationId id, T payload) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> future = sendAsync(topic, key, id, payload);
        future.get();
    }

    public Future<RecordMetadata> sendAsync(String topic, String key, CorrelationId id, T payload) {
        var message = new Message<>(id, payload);
        var record = new ProducerRecord<>(topic, key, message);

        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("[SUCCESS] TOPIC::" + data.topic() + ":::PARTITION->" + data.partition() + "/OFFSET->" + data.offset() + "/TIMESTAMP->" + data.timestamp());
        };

        return producer.send(record, callback);
    }


    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());

        // esta configuração define que o produtor deve esperar a confirmação de todas as réplicas DISPONÍVEIS antes de
        // considerar a mensagem enviada com sucesso. Há um porém: mesmo que tenhamos, por exemplo, um replication
        // factor de 3, mas no momento da mensagem apenas uma dessas réplicas esteja disponível, será considerado
        // sucesso pois há apenas 1 in-sync replica (ISR) e isso corresponde a "todas as réplicas" NAQUELE INSTANTE.
        // ou seja, não quer dizer que a mensagem precisa ser confirmada por todas as réplicas e sim pelas réplicas
        // disponíveis (que não caíram/travaram/etc).
        //
        // podemos usar a config `min.insync.replicas` para "exigir" que pelo menos o líder e N réplicas confirmarem
        // a gravação da mensagem. Ex: se min.insync.replicas=2, então o produtor só considerará que a mensagem foi
        // enviada com sucesso se o líder e pelo menos uma réplica confirmar. Nesse caso, o produtor deve receber um
        // erro de NotEnoughReplicasException ou NotEnoughReplicasAfterAppendException;
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        return properties;
    }

    @Override
    public void close() {
        this.producer.close();
    }
}
