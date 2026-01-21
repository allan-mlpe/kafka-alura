package br.com.alura.ecommerce;

import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var orderDispatcher = new KafkaDispatcher<Order>()) {
            try(var emailDispatcher = new KafkaDispatcher<Email>()) {
                for (var i = 0; i < 10; i++) {
                    var orderId = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() * 5000 + 1);
                    var email = Math.random() + "@email.com";

                    var order = new Order(orderId, amount, email);
                    var id = new CorrelationId(NewOrderMain.class.getSimpleName());

                    orderDispatcher.send(
                            "ECOMMERCE_NEW_ORDER",
                            email,
                            id,
                            order);

                    var successEmail = new Email("Thanks for your order!", "We're processing your products.");
                    emailDispatcher.send(
                            "ECOMMERCE_SEND_EMAIL",
                            email,
                            id,
                            successEmail);
                }
            }
        }
    }
}
