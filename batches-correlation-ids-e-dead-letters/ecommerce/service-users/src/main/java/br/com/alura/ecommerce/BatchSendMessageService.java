package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {

    private final Connection connection;
    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();

    BatchSendMessageService() throws SQLException {
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
        var batchService = new BatchSendMessageService();
        try(var service = new KafkaService<>(
                BatchSendMessageService.class.getSimpleName(),
                "SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                String.class,
                Map.of())) {

            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<String>> record) throws SQLException, ExecutionException, InterruptedException {
        System.out.println("------------------------------------------------");
        System.out.println("Processing new batch");
        System.out.println("- Value::" + record.value());

        var users = getAllUsers();
        var message = record.value();
        var topic = message.getPayload();

        for (User user : users) {
            userDispatcher.send(
                    topic,
                    user.uuid(),
                    message.getId().continueWith(BatchSendMessageService.class.getSimpleName()),
                    user);
        }
    }

    private List<User> getAllUsers() throws SQLException {
        var resultSet = connection.prepareStatement("select uuid from Users").executeQuery();

        List<User> users = new ArrayList<>();
        while(resultSet.next()) {
            var userId = resultSet.getString(1);
            var user = new User(userId);

            users.add(user);
        }
        return users;
    }
}
