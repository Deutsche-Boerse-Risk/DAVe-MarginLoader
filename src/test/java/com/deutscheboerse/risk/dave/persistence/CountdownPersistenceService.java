package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.model.*;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;

import java.util.List;

public class CountdownPersistenceService implements PersistenceService {

    private final Async async;
    private JsonObject lastModel;

    public CountdownPersistenceService(Async async) {
        this.async = async;
    }

    @Override
    public void initialize(Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void storeAccountMargin(List<AccountMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models, resultHandler);
    }

    @Override
    public void storeLiquiGroupMargin(List<LiquiGroupMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models, resultHandler);
    }

    @Override
    public void storeLiquiGroupSplitMargin(List<LiquiGroupSplitMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models, resultHandler);
    }

    @Override
    public void storePoolMargin(List<PoolMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models, resultHandler);
    }

    @Override
    public void storePositionReport(List<PositionReportModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models, resultHandler);
    }

    @Override
    public void storeRiskLimitUtilization(List<RiskLimitUtilizationModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models, resultHandler);
    }

    @Override
    public void close() {
    }

    private void store(List<? extends JsonObject> models, Handler<AsyncResult<Void>> resultHandler) {

        for (JsonObject model: models) {
            this.lastModel = model;
            this.async.countDown();
        }

        // Always succeeds
        resultHandler.handle(Future.succeededFuture());
    }

    public JsonObject getLastModel() {
        return this.lastModel;
    }
}
