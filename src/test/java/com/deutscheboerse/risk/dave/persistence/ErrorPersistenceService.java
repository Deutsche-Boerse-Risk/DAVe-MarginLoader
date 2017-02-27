package com.deutscheboerse.risk.dave.persistence;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.serviceproxy.ServiceException;

public class ErrorPersistenceService implements PersistenceService {
    private static final Logger LOG = LoggerFactory.getLogger(ErrorPersistenceService.class);

    private final Vertx vertx;
    private final JsonObject config;

    public ErrorPersistenceService(Vertx vertx) {
        this.vertx = vertx;
        this.config = vertx.getOrCreateContext().config();
    }

    @Override
    public void initialize(JsonObject config, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void storeAccountMargin(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }

    @Override
    public void storeLiquiGroupMargin(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }

    @Override
    public void storeLiquiGroupSplitMargin(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }

    @Override
    public void storePoolMargin(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }

    @Override
    public void storePositionReport(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }

    @Override
    public void storeRiskLimitUtilization(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }
}
