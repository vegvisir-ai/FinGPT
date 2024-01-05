package com.vegvisir.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.vegvisir.common.LogUtil;

import static com.vegvisir.common.LogUtil.LogLevel.*;

@SuppressWarnings("unused")
public class RankingHandler implements RequestHandler<String, String> {

    @Override
    public String handleRequest(String request, Context ctx) {
        LambdaLogger logger = ctx.getLogger();
        LogUtil.log(logger, INFO, "Received ranking request: " + request);

        //TODO: Implement ranking logic

        return null;
    }
}
