package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.model.*;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;

public class CountdownPersistenceService implements PersistenceService {
    private static final Logger LOG = LoggerFactory.getLogger(CountdownPersistenceService.class);

    private final Vertx vertx;
    private final Async async;
    private JsonObject lastMessage;

    public CountdownPersistenceService(Vertx vertx, Async async) {
        this.vertx = vertx;
        this.async = async;
    }

    @Override
    public void initialize(JsonObject config, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void storeAccountMargin(AccountMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, resultHandler);
    }

    @Override
    public void storeLiquiGroupMargin(LiquiGroupMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, resultHandler);
    }

    @Override
    public void storeLiquiGroupSplitMargin(LiquiGroupSplitMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, resultHandler);
    }

    @Override
    public void storePoolMargin(PoolMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, resultHandler);
    }

    @Override
    public void storePositionReport(PositionReportModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, resultHandler);
    }

    @Override
    public void storeRiskLimitUtilization(RiskLimitUtilizationModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, resultHandler);
    }

    @Override
    public void close() {
    }

    private void store(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {

        // Store the message
        this.lastMessage = message;

        this.async.countDown();

        // Always succeeds
        resultHandler.handle(Future.succeededFuture());
    }

    public JsonObject getLastMessage() {
        return this.lastMessage;
    }
}
