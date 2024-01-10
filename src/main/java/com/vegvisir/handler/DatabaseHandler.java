package com.vegvisir.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.vegvisir.common.DBManager;
import com.vegvisir.common.LogUtil;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

import static com.vegvisir.common.LogUtil.LogLevel.*;

@SuppressWarnings("unused")
public class DatabaseHandler implements RequestHandler<String, String> {

    @Override
    public String handleRequest(String request, Context ctx) {
        LambdaLogger logger = ctx.getLogger();
        LogUtil.log(logger, INFO, "Received database request: " + request);

        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            conn = DBManager.getConnection();
            DBManager.initializeDB(conn);
            LogUtil.log(logger, INFO, "Get DB connection success");

            preparedStatement = conn.prepareStatement(request);
            if (request.startsWith("SELECT")) {
                resultSet = preparedStatement.executeQuery();
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                int columnCnt = resultSetMetaData.getColumnCount();
                while (resultSet.next()) {
                    for (int i = 1; i <= columnCnt; i++) {
                        String columnName = resultSetMetaData.getColumnName(i);
                        Object columnVal = resultSet.getObject(i);
                        LogUtil.log(logger, INFO, columnName + ": " + columnVal);
                    }
                }
            } else {
                preparedStatement.executeUpdate();
            }
            LogUtil.log(logger, INFO, "Execute SQL statement success");
        } catch (Exception e) {
            LogUtil.log(logger, ERROR, "Handle database request error: " + e.getMessage());
            return null;
        } finally {
            List<Statement> delStatements = new LinkedList<>();
            delStatements.add(preparedStatement);
            List<ResultSet> delResultSets = new LinkedList<>();
            delResultSets.add(resultSet);
            DBManager.closeResource(logger, conn, delStatements, delResultSets);
        }

        return "Handle database request success";
    }
}
