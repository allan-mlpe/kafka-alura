package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.*;
import java.util.Map;

public class CreateUserService {

    private final Connection connection;

    CreateUserService() throws SQLException {
        String url = "jdbc:sqlite:users_database.db";
        this.connection = DriverManager.getConnection(url);

        // cria a tabela
        connection.createStatement().execute("create table Users (" +
                "uuid varchar(200) primary key," +
                "email varchar(200))");
    }

    public static void main(String[] args) throws SQLException {
        var createUserService = new CreateUserService();
        try(var service = new KafkaService<>(
                CreateUserService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                createUserService::parse,
                Order.class,
                Map.of())) {

            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) throws SQLException {
        System.out.println("------------------------------------------------");
        System.out.println("Processing new order, checking for new user...");
        System.out.println("- Value::" + record.value().toString());

        var order = record.value();

        if (isNewUser(order.email())) {
            insertUser(order);
        }

    }

    private void insertUser(Order order) throws SQLException {
        PreparedStatement insert =
                connection.prepareStatement("insert into Users (uuid, email) values (?, ?)");

        insert.setString(1, "uuid");
        insert.setString(2, "email");
        insert.execute();

        System.out.println(String.format("Usuário %s com email %s adicionado", order.userId(), order.email()));
    }

    private boolean isNewUser(String email) throws SQLException {
        var query = connection.prepareStatement("select uuid from Users where email = ? limit 1");
        query.setString(1, email);

        ResultSet results = query.executeQuery();

        return !results.next();
    }
}
