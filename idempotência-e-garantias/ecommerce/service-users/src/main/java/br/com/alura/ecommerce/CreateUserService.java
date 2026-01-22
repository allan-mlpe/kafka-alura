package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.database.LocalDatabase;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.*;
import java.util.UUID;

public class CreateUserService implements ConsumerService<Order> {

    private final LocalDatabase database;

    CreateUserService() throws SQLException {
        this.database = new LocalDatabase("users_database");
        this.database.createIfNotExists("create table Users (" +
                "uuid varchar(200) primary key," +
                "email varchar(200))");
    }

    public static void main(String[] args) {
        new ServiceRunner<>(CreateUserService::new).start(1);
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        System.out.println("------------------------------------------------");
        System.out.println("Processing new order, checking for new user...");
        System.out.println("- Value::" + record.value().toString());

        var message = record.value();
        var order = message.getPayload();
        var email = order.email();

        if (isNewUser(email)) {
            insertUser(email);
        }
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserService.class.getSimpleName();
    }

    private void insertUser(String email) throws SQLException {
        var userId = UUID.randomUUID().toString();
        database.update("insert into Users (uuid, email) values (?, ?)", userId, email);

        System.out.println(String.format("Usuário %s com email %s adicionado", userId, email));
    }

    private boolean isNewUser(String email) throws SQLException {
        ResultSet results = database.query("select uuid from Users where email = ? limit 1", email);

        return !results.next();
    }
}
