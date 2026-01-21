package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService implements ConsumerService<Email> {

    public void parse(ConsumerRecord<String, Message<Email>> record) {
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

    public String getConsumerGroup() {
        return EmailService.class.getSimpleName();
    }

    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }

    public static void main(String[] args) {
        new ServiceRunner<>(EmailService::new).start(5);
    }
}
