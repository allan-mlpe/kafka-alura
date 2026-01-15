package br.com.alura.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var dispatcher = new KafkaDispatcher()) {
            for(var i = 0; i < 10; i++) {
                // a key é fundamental para garantir a distribuição
                // das mensagens entre as partições existentes
                String newOrderKey = UUID.randomUUID().toString();

                String value = "123123,pedido1,309183091301";
                var body = "Thanks for your order! We're processing your products.";

                dispatcher.send("ECOMMERCE_NEW_ORDER", newOrderKey, value);
                dispatcher.send("ECOMMERCE_SEND_EMAIL", newOrderKey, body);
            }
        }
    }
}
