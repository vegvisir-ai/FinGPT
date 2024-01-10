package com.vegvisir.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.Gson;
import com.vegvisir.common.DBManager;
import com.vegvisir.common.LogUtil;
import com.vegvisir.common.entity.Customer;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static com.vegvisir.common.LogUtil.LogLevel.ERROR;
import static com.vegvisir.common.LogUtil.LogLevel.INFO;

@SuppressWarnings("unused")
public class RegisterHandler implements RequestHandler<String, String> {

    @Override
    public String handleRequest(String request, Context ctx) {
        LambdaLogger logger = ctx.getLogger();
        LogUtil.log(logger, INFO, "Received register request: " + request);

        Customer c = new Gson().fromJson(request, Customer.class);

        Connection conn = null;
        PreparedStatement updateStatement = null;
        try {
            conn = DBManager.getConnection();
            DBManager.initializeDB(conn);
            LogUtil.log(logger, INFO, "Get DB connection success");

            updateStatement = conn.prepareStatement("INSERT INTO customer VALUES (?, ?, ?, ?, ?);");
            UUID uuid = UUID.randomUUID();
            Array history = conn.createArrayOf("BIGINT", c.history().toArray(new Long[0]));
            updateStatement.setObject(1, uuid, Types.OTHER);
            updateStatement.setString(2, c.username());
            updateStatement.setString(3, c.email());
            updateStatement.setString(4, c.stripeToken());
            updateStatement.setArray(5, history);
            updateStatement.executeUpdate();
            LogUtil.log(logger, INFO, "Store customer info into DB success");
        } catch (Exception e) {
            LogUtil.log(logger, ERROR, "Handle register request error: " + e.getMessage());
            return null;
        } finally {
            List<Statement> delStatements = new LinkedList<>();
            delStatements.add(updateStatement);
            DBManager.closeResource(logger, conn, delStatements, null);
        }

        LogUtil.log(logger, INFO, "Handle register request success");
        return "Handle register request success";
    }
}
