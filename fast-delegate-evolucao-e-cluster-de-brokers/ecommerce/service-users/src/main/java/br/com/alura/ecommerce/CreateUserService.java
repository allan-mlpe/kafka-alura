package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.*;
import java.util.Map;
import java.util.UUID;

public class CreateUserService {

    private final Connection connection;

    CreateUserService() throws SQLException {
        String url = "jdbc:sqlite:users_database.db";
        this.connection = DriverManager.getConnection(url);

        // cria a tabela
        try {
            connection.createStatement().execute("create table Users (" +
                    "uuid varchar(200) primary key," +
                    "email varchar(200))");
        } catch (SQLException e) {
            // be careful, the sql could be wrong.
            // we're doing like this for "educational purposes" to focus on Kafka
            e.printStackTrace();
        }

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
        var email = order.email();

        if (isNewUser(email)) {
            insertUser(email);
        }

    }

    private void insertUser(String email) throws SQLException {
        PreparedStatement insert =
                connection.prepareStatement("insert into Users (uuid, email) values (?, ?)");
        var userId = UUID.randomUUID().toString();

        insert.setString(1, userId);
        insert.setString(2, email);
        insert.execute();

        System.out.println(String.format("Usuário %s com email %s adicionado", userId, email));
    }

    private boolean isNewUser(String email) throws SQLException {
        var query = connection.prepareStatement("select uuid from Users where email = ? limit 1");
        query.setString(1, email);

        ResultSet results = query.executeQuery();

        return !results.next();
    }
}
