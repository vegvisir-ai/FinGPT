package com.vegvisir.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.GsonBuilder;
import com.vegvisir.common.DBManager;
import com.vegvisir.common.LogUtil;
import com.vegvisir.common.entity.Ticker;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.vegvisir.common.LogUtil.LogLevel.*;

@SuppressWarnings("unused")
public class RankingHandler implements RequestHandler<String, String> {

    static class TickerSerializationStrategy implements ExclusionStrategy {
        @Override
        public boolean shouldSkipField(FieldAttributes f) {
            return f.getName().equals("rate");
        }

        @Override
        public boolean shouldSkipClass(Class<?> clazz) {
            return false;
        }
    }

    @Override
    public String handleRequest(String request, Context ctx) {
        LambdaLogger logger = ctx.getLogger();
        LogUtil.log(logger, INFO, "Received ranking request: " + request);

        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Ticker> response = new LinkedList<>();

        try {
            conn = DBManager.getConnection();
            DBManager.initializeDB(conn);
            LogUtil.log(logger, INFO, "Get DB connection success");

            preparedStatement = conn.prepareStatement("SELECT * FROM ranking");
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String tickerId = resultSet.getString("ticker_id");
                String retrievedInfo = resultSet.getString("retrieved_info");
                String predictionResult = resultSet.getString("prediction_result");

                Pattern pattern = Pattern.compile("Prediction: (Up|Down)\\s+by\\s+\\d+(\\.\\d+)?-\\d+(\\.\\d+)?%");
                Matcher matcher = pattern.matcher(predictionResult);
                if (matcher.find()) {
                    String match = matcher.group();
                    int sign = match.substring(12, 14).equalsIgnoreCase("up") ? 1 : -1;
                    String low = match.substring(match.indexOf("by") + 3, match.indexOf("-"));
                    String high = match.substring(match.indexOf("-") + 1, match.indexOf("%"));
                    double rate = (Double.parseDouble(low) + Double.parseDouble(high)) * sign / 2;
                    Ticker ticker = new Ticker(tickerId, retrievedInfo, predictionResult, rate);
                    response.add(ticker);
                }
                response.sort(Comparator.comparingDouble(Ticker::rate));
            }
            LogUtil.log(logger, INFO, "Retrieve ranking result from DB success, number of tickers: "
                    + response.size());
        } catch (Exception e) {
            LogUtil.log(logger, ERROR, "Handle ranking request error: " + e.getMessage());
            return null;
        } finally {
            List<Statement> delStatements = new LinkedList<>();
            delStatements.add(preparedStatement);
            List<ResultSet> delResultSets = new LinkedList<>();
            delResultSets.add(resultSet);
            DBManager.closeResource(logger, conn, delStatements, delResultSets);
        }

        GsonBuilder gsonBuilder = new GsonBuilder().addSerializationExclusionStrategy(new TickerSerializationStrategy());
        LogUtil.log(logger, INFO, "Configure custom JSON serialization strategy success");
        return gsonBuilder.create().toJson(response);
    }
}
