package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties());

        // mensagem para novo pedido
        String value = "123123,pedido1,309183091301";
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);

        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("[SUCCESS] TOPIC::" + data.topic() + ":::PARTITION->" + data.partition() + "/OFFSET->" + data.offset() + "/TIMESTAMP->" + data.timestamp());
        };

        // o método send retorna um Future, não nos deixando saber se a msg foi enviada com sucesos
        // por isso estamos passando um callback para ele
        producer.send(record, callback).get();

        // mensagem para enviar email
        var title = "Thanks for your order";
        var body = "We're processing your products.";
        var emailRecord = new ProducerRecord<String, String>("ECOMMERCE_SEND_EMAIL", title, body);

        producer.send(emailRecord, callback).get();
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
