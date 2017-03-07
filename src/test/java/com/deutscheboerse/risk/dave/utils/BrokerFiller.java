package com.deutscheboerse.risk.dave.utils;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface BrokerFiller {
    void setUpAllQueues(Handler<AsyncResult<String>> handler);
    void setUpAccountMarginQueue(Handler<AsyncResult<String>> handler);
    void setUpLiquiGroupMarginQueue(Handler<AsyncResult<String>> handler);
    void setUpLiquiGroupSplitMarginQueue(Handler<AsyncResult<String>> handler);
    void setUpPoolMarginQueue(Handler<AsyncResult<String>> handler);
    void setUpPositionReportQueue(Handler<AsyncResult<String>> handler);
    void setUpRiskLimitUtilizationQueue(Handler<AsyncResult<String>> handler);
}
