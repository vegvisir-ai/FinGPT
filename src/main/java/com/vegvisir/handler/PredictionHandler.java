package com.vegvisir.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.Gson;
import com.vegvisir.common.DBManager;
import com.vegvisir.common.HttpUtil;
import com.vegvisir.common.entity.PredictionRequest;
import com.vegvisir.common.entity.PredictionResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

@SuppressWarnings({"unused"})
public class PredictionHandler implements RequestHandler<String, String> {
    private static final String FinGPTHost = System.getenv("FIN_GPT_HOST");
    private static final String FinGPTPort = System.getenv("FIN_GPT_PORT");

    @Override
    public String handleRequest(String request, Context ctx) {
        LambdaLogger logger = ctx.getLogger();
        logger.log("Received register request: " + request);

        PredictionRequest predictionRequest = new Gson().fromJson(request, PredictionRequest.class);

        Connection conn = null;
        PreparedStatement updateStatement = null;
        try {
            conn = DBManager.getConnection();
            DBManager.initializeDB(conn);
            logger.log("Get DB connection success");

            String predictAddr = String.format("https://%s:%s/api/v1/predict", FinGPTHost, FinGPTPort);
            String result = HttpUtil.sendHttpRequest(predictAddr, "POST", request);
            PredictionResult predictionResult = new Gson().fromJson(result, PredictionResult.class);

            updateStatement = conn.prepareStatement("INSERT INTO ticker VALUES (DEFAULT, ?, ?, ?, ?, ?, ?);");
            updateStatement.setString(1, predictionRequest.tickerId());
            updateStatement.setString(2, predictionResult.retrievedInfo());
            updateStatement.setString(3, predictionResult.predictionResult());
            updateStatement.setInt(4, predictionRequest.period());
            updateStatement.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()));
            updateStatement.setLong(6, predictionRequest.customerId());
            updateStatement.executeUpdate();
        } catch (ClassNotFoundException e) {
            logger.log("Load DB driver error: " + e.getMessage());
        } catch (SQLException e) {
            logger.log("Execute SQL statement error: " + e.getMessage());
        } catch (Exception e) {
            logger.log("Handle prediction request error: " + e.getMessage());
        } finally {
            if (updateStatement != null) {
                try {
                    updateStatement.close();
                } catch (SQLException e) {
                    logger.log("Close SQL statement error: " + e.getMessage());
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.log("Close DB connection error: " + e.getMessage());
                }
            }
        }

        return "Handle prediction request success";
    }
}
