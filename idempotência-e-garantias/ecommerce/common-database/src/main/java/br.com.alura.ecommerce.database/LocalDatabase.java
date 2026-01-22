package br.com.alura.ecommerce.database;

import java.sql.*;

public class LocalDatabase {

    private final Connection connection;

    public LocalDatabase(String database) throws SQLException {
        String url = "jdbc:sqlite:" + database + ".db";
        this.connection = DriverManager.getConnection(url);
    }

    // da forma como está, apesar de genérico, estamos vulneráveis a SQL Injection.
    // vamos manter assim apenas por simplicidade e para focar no Kafka.
    public void createIfNotExists(String sql) {
        // cria a tabela
        try {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            // be careful, the sql could be wrong.
            // we're doing like this for "educational purposes" to focus on Kafka
            e.printStackTrace();
        }
    }
    
    private PreparedStatement prepare(String statement, String[] params) throws SQLException {
        PreparedStatement preparedStatement =
                connection.prepareStatement(statement);

        for (var i = 0; i < params.length; i++) {
            preparedStatement.setString(i+1, params[i]);
        }
        return preparedStatement;
    }

    public void update(String statement, String ... params) throws SQLException {
        prepare(statement, params).execute();
    }

    public ResultSet query(String query, String ... params) throws SQLException {
        return prepare(query, params).executeQuery();
    }
}
