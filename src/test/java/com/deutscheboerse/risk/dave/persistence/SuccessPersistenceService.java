package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import com.deutscheboerse.risk.dave.model.*;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import javax.inject.Inject;
import java.util.List;

public class SuccessPersistenceService implements PersistenceService {

    private final HealthCheck healthCheck;

    @Inject
    public SuccessPersistenceService(Vertx vertx) {
        this.healthCheck = new HealthCheck(vertx);
    }

    @Override
    public void initialize(Handler<AsyncResult<Void>> resultHandler) {
        healthCheck.setComponentReady(HealthCheck.Component.PERSISTENCE_SERVICE);
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void storeAccountMargin(List<AccountMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void storeLiquiGroupMargin(List<LiquiGroupMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void storeLiquiGroupSplitMargin(List<LiquiGroupSplitMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void storePoolMargin(List<PoolMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void storePositionReport(List<PositionReportModel> models, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void storeRiskLimitUtilization(List<RiskLimitUtilizationModel> models, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void close() {

    }
}
