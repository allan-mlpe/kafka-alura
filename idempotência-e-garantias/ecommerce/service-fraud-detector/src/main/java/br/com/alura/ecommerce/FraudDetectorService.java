package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.database.LocalDatabase;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService implements ConsumerService<Order> {
    private static final BigDecimal FRAUD_THRESHOLD_VALUE = new BigDecimal(4200);
    private final KafkaDispatcher<Order> orderKafkaDispatcher = new KafkaDispatcher<>();;
    private final LocalDatabase database;

    public FraudDetectorService() throws SQLException {
        this.database = new LocalDatabase("fraud_db");
        this.database.createIfNotExists("create table Orders (" +
                "uuid varchar(200) primary key," +
                "is_fraud boolean)");
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }

    public static void main(String[] args) {
        new ServiceRunner<>(FraudDetectorService::new).start(1);
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
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

        if (wasProcessed(order)) {
            System.out.println("Order was already processed.");
            return;
        }

        if (order.amount().compareTo(FRAUD_THRESHOLD_VALUE) >= 0) {
            database.update("insert into Orders (uuid, is_fraud) values (?, true)", order.orderId());
            // pretending that the fraud happens when the amount is >= threshold
            orderKafkaDispatcher.send(
                    "ECOMMERCE_ORDER_REJECTED",
                    order.email(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
            System.out.println("Order rejected!" + order);
        } else {
            database.update("insert into Orders (uuid, is_fraud) values (?, false)", order.orderId());
            // order accepted
            orderKafkaDispatcher.send(
                    "ECOMMERCE_ORDER_ACCEPTED",
                    order.email(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
            System.out.println("Order approved!" + order);
        }
    }

    private boolean wasProcessed(Order order) throws SQLException {
        ResultSet query = database.query(
                "select uuid from Orders where uuid = ? limit 1",
                order.orderId()
        );

        return query.next();
    }
}
