package com.deutscheboerse.risk.dave.persistence;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.serviceproxy.ServiceException;

public class InitPersistenceService implements PersistenceService {
    private static final Logger LOG = LoggerFactory.getLogger(InitPersistenceService.class);

    private final Vertx vertx;
    private final boolean succeeds;
    private boolean initialized = false;

    public InitPersistenceService(Vertx vertx, boolean succeeds) {
        this.vertx = vertx;
        this.succeeds = succeeds;
    }

    @Override
    public void initialize(JsonObject config, Handler<AsyncResult<Void>> resultHandler) {
        if (this.succeeds) {
            this.initialized = true;
            resultHandler.handle(Future.succeededFuture());
        } else {
            this.initialized = false;
            resultHandler.handle(ServiceException.fail(INIT_ERROR, "Init failed"));
        }
    }

    @Override
    public void storeAccountMargin(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Store not implemented"));
    }

    @Override
    public void storeLiquiGroupMargin(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Store not implemented"));
    }

    @Override
    public void storeLiquiGroupSplitMargin(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Store not implemented"));
    }

    @Override
    public void storePoolMargin(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Store not implemented"));
    }

    @Override
    public void storePositionReport(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Store not implemented"));
    }

    @Override
    public void storeRiskLimitUtilization(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Store not implemented"));
    }

    public boolean isInitialized() {
        return this.initialized;
    }
}
