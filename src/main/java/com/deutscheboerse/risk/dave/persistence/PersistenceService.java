package com.deutscheboerse.risk.dave.persistence;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

@ProxyGen
public interface PersistenceService {
    String SERVICE_ADDRESS = "persistenceService";

    int INIT_ERROR = 2;
    int STORE_ERROR = 3;

    void initialize(JsonObject config, Handler<AsyncResult<Void>> resultHandler);

    void storeAccountMargin(JsonObject message, Handler<AsyncResult<Void>> resultHandler);
    void storeLiquiGroupMargin(JsonObject message, Handler<AsyncResult<Void>> resultHandler);
    void storeLiquiGroupSplitMargin(JsonObject message, Handler<AsyncResult<Void>> resultHandler);
    void storePoolMargin(JsonObject message, Handler<AsyncResult<Void>> resultHandler);
    void storePositionReport(JsonObject message, Handler<AsyncResult<Void>> resultHandler);
    void storeRiskLimitUtilization(JsonObject message, Handler<AsyncResult<Void>> resultHandler);
}
