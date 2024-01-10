package com.vegvisir.common;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.vegvisir.common.entity.Customer;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static com.vegvisir.common.LogUtil.LogLevel.*;

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
                        id UUID PRIMARY KEY,
                        username TEXT NOT NULL,
                        email TEXT NOT NULL,
                        stripe_token TEXT,
                        history INTEGER[],
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
                        customer_id UUID,
                        CONSTRAINT customer_id_foreign_key FOREIGN KEY (customer_id) REFERENCES customer (id)
                    );""";
        String createRankingTable = """
                    CREATE TABLE IF NOT EXISTS ranking (
                        ticker_id TEXT PRIMARY KEY,
                        retrieved_info TEXT,
                        prediction_result TEXT NOT NULL
                    )""";
        setupStatement.addBatch(createCustomerTable);
        setupStatement.addBatch(createTickerTable);
        setupStatement.addBatch(createRankingTable);
        setupStatement.executeBatch();
        setupStatement.close();
    }

    public static Customer selectCustomerByEmail(LambdaLogger logger, Connection conn, String email) throws SQLException {
        LogUtil.log(logger, INFO, "Start to retrieve customer info from database");

        PreparedStatement preparedStatement = conn.prepareStatement("SELECT * FROM customer WHERE email = ?;");
        preparedStatement.setString(1, email);
        ResultSet resultSet = preparedStatement.executeQuery();
        UUID id = null;
        String username = null, stripeToken = null;
        List<Long> history = null;
        while (resultSet.next()) {
            id = resultSet.getObject(1, UUID.class);
            username = resultSet.getString(2);
            stripeToken = resultSet.getString(4);
            history = Arrays.asList((Long[])resultSet.getArray(5).getArray());
        }
        resultSet.close();
        preparedStatement.close();
        if (id != null) {
            return new Customer(id, username, email, stripeToken, history);
        }
        return null;
    }

    public static void closeResource(LambdaLogger logger, Connection conn, List<Statement> statements,
                                     List<ResultSet> resultSets) {
        if (resultSets != null) {
            for (ResultSet rs: resultSets) {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException e) {
                        LogUtil.log(logger, ERROR, "Close ResultSet error: " + e.getMessage());
                    }
                }
            }
        }
        if (statements != null) {
            for (Statement st: statements) {
                if (st != null) {
                    try {
                        st.close();
                    } catch (SQLException e) {
                        LogUtil.log(logger, ERROR, "Close SQL statement error: " + e.getMessage());
                    }
                }
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                LogUtil.log(logger, ERROR, "Close DB connection error: " + e.getMessage());
            }
        }
    }
}
