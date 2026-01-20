package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {
    private static final BigDecimal FRAUD_THRESHOLD_VALUE = new BigDecimal(4200);
    private final KafkaDispatcher<Order> orderKafkaDispatcher = new KafkaDispatcher<>();;

    public static void main(String[] args) {
        var fraudService = new FraudDetectorService();
        try(var service = new KafkaService<>(
                FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse,
                Map.of())) {

            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
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

        var message = record.value();
        var order = message.getPayload();

        if (order.amount().compareTo(FRAUD_THRESHOLD_VALUE) >= 0) {
            // pretending that the fraud happens when the amount is >= threshold
            orderKafkaDispatcher.send(
                    "ECOMMERCE_ORDER_REJECTED",
                    order.email(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
            System.out.println("Order rejected!" + order);
        } else {
            // order accepted
            orderKafkaDispatcher.send(
                    "ECOMMERCE_ORDER_ACCEPTED",
                    order.email(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
            System.out.println("Order processed!" + order);
        }
    }
}
