package com.vegvisir.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DBManager {
    private static final String hostname = System.getenv("RDS_HOSTNAME");
    private static final String port = System.getenv("RDS_PORT");
    private static final String dbName = System.getenv("RDS_DB_NAME");
    private static final String userName = System.getenv("RDS_USERNAME");
    private static final String password = System.getenv("RDS_PASSWORD");

    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        String jdbcUrl = "jdbc:postgresql://" + hostname + ":" + port + "/" + dbName;
        Class.forName("org.postgresql.Driver");
        return DriverManager.getConnection(jdbcUrl, userName, password);
    }

    public static void initializeDB(Connection conn) throws SQLException {
        Statement setupStatement = conn.createStatement();
        String createCustomerTable = """
                    CREATE TABLE IF NOT EXISTS customer (
                        id SERIAL PRIMARY KEY,
                        username TEXT NOT NULL,
                        email TEXT NOT NULL,
                        stripe_token TEXT,
                        CONSTRAINT customer_email_unique UNIQUE(email)
                    );""";
        String createTickerTable = """
                    CREATE TABLE IF NOT EXISTS ticker (
                        id SERIAL PRIMARY KEY,
                        ticker_id TEXT NOT NULL,
                        retrieved_info TEXT,
                        prediction_result TEXT NOT NULL,
                        period INTEGER NOT NULL,
                        prediction_time TIMESTAMP NOT NULL,
                        customer_id BIGINT,
                        CONSTRAINT customer_id_foreign_key FOREIGN KEY (customer_id) REFERENCES customer (id)
                    );""";
        setupStatement.addBatch(createCustomerTable);
        setupStatement.addBatch(createTickerTable);
        setupStatement.executeBatch();
        setupStatement.close();
    }
}
