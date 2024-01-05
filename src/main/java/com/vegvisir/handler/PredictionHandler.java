package com.vegvisir.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.Gson;
import com.vegvisir.common.DBManager;
import com.vegvisir.common.HttpUtil;
import com.vegvisir.common.LogUtil;
import com.vegvisir.common.entity.PredictionRequest;
import com.vegvisir.common.entity.PredictionResponse;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

import static com.vegvisir.common.LogUtil.LogLevel.*;

@SuppressWarnings({"unused"})
public class PredictionHandler implements RequestHandler<String, String> {
    private static final String FinGPTHost = System.getenv("FIN_GPT_HOST");
    private static final String FinGPTPort = System.getenv("FIN_GPT_PORT");

    @Override
    public String handleRequest(String request, Context ctx) {
        LambdaLogger logger = ctx.getLogger();
        LogUtil.log(logger, INFO, "Received register request: " + request);

        PredictionRequest predictionRequest = new Gson().fromJson(request, PredictionRequest.class);

        Connection conn = null;
        PreparedStatement updateStatement = null;
        String response;
        try {
            conn = DBManager.getConnection();
            DBManager.initializeDB(conn);
            LogUtil.log(logger, INFO, "Get DB connection success");

            String predictAddr = String.format("https://%s:%s/api/v1/predict", FinGPTHost, FinGPTPort);
            response = HttpUtil.sendHttpRequest(predictAddr, "POST", request);
            PredictionResponse predictionResponse = new Gson().fromJson(response, PredictionResponse.class);
            LogUtil.log(logger, INFO, "Invoke FinGPT predict API success, result: " + predictionResponse.predictionResult());

            updateStatement = conn.prepareStatement("INSERT INTO ticker VALUES (DEFAULT, ?, ?, ?, ?, ?, ?);");
            updateStatement.setString(1, predictionRequest.tickerId());
            updateStatement.setString(2, predictionResponse.retrievedInfo());
            updateStatement.setString(3, predictionResponse.predictionResult());
            updateStatement.setInt(4, predictionRequest.period());
            updateStatement.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()));
            updateStatement.setLong(6, predictionRequest.customerId());
            updateStatement.executeUpdate();
            LogUtil.log(logger, INFO, "Update ticker table success");
        } catch (Exception e) {
            LogUtil.log(logger, ERROR, "Handle prediction request error: " + e.getMessage());
            return null;
        } finally {
            List<Statement> delResources = new LinkedList<>();
            delResources.add(updateStatement);
            DBManager.closeResource(logger, conn, delResources, null);
        }

        LogUtil.log(logger, INFO, "Handle prediction request success");
        return response;
    }
}
