package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailNewOrderService implements ConsumerService<Order> {
    private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>();;

    public static void main(String[] args) {
        new ServiceRunner<>(EmailNewOrderService::new).start(1);
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("------------------------------------------------");
        System.out.println("Processing new order, preparing email");
        System.out.println("- Key::" + record.key());
        System.out.println("- Value::" + record.value());
        System.out.println("- Partition::" + record.partition());
        System.out.println("- Offset::" + record.offset());
        System.out.println("- Topic::" + record.topic());

        Message<Order> message = record.value();
        var order = message.getPayload();
        var userEmail = order.email();

        var id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());

        var successEmail = new Email("Thanks for your order!", "We're processing your products.");
        emailDispatcher.send(
                "ECOMMERCE_SEND_EMAIL",
                userEmail,
                id,
                successEmail);
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }
}
