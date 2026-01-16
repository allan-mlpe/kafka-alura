package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {
    private static final BigDecimal FRAUD_THRESHOLD_VALUE = new BigDecimal(4200);
    private KafkaDispatcher<Object> orderKafkaDispatcher;

    public static void main(String[] args) {
        var fraudService = new FraudDetectorService();
        try(var service = new KafkaService<>(
                FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse,
                Order.class,
                Map.of())) {

            service.run();
        }

        fraudService.orderKafkaDispatcher = new KafkaDispatcher<>();
    }

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("------------------------------------------------");
        System.out.println("Processing new order, checking for fraud...");
        System.out.println("- Key::" + record.key());
        System.out.println("- Value::" + record.value().toString());
        System.out.println("- Partition::" + record.partition());
        System.out.println("- Offset::" + record.offset());
        System.out.println("- Topic::" + record.topic());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }

        var order = record.value();

        if (order.amount().compareTo(FRAUD_THRESHOLD_VALUE) >= 0) {
            // pretending that the fraud happens when the amount is >= threshold
            orderKafkaDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.userId(), order);
        } else {
            // order accepted
            orderKafkaDispatcher.send("ECOMMERCE_ORDER_ACCEPTED", order.userId(), order);
        }

        System.out.println("Order processed!");
    }
}
