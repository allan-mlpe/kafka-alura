package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class EmailService {

    private void parse(ConsumerRecord<String, Message<Email>> record) {
        System.out.println("------------------------------------------------");
        System.out.println("::: SendEmail Service :::");
        System.out.println("- Key::" + record.key());
        System.out.println("- Value::" + record.value().toString());
        System.out.println("- Partition::" + record.partition());
        System.out.println("- Offset::" + record.offset());
        System.out.println("- Topic::" + record.topic());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }

        System.out.println("Email sent!");
    }


    public static void main(String[] args) {
        var emailService = new EmailService();
        try(var service = new KafkaService<>(
                EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                Email.class,
                Map.of())) {

            service.run();
        }
    }
}
