package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.model.*;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.unit.Async;
import io.vertx.serviceproxy.ServiceException;

import java.util.List;

public class CircuitBreakerPersistenceService implements PersistenceService {

    private final Async async;
    private final int numberOfFailures;
    private int counter = 0;

    public CircuitBreakerPersistenceService(Async async, int numberOfFailures) {
        this.async = async;
        this.numberOfFailures = numberOfFailures;
    }

    @Override
    public void initialize(Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void storeAccountMargin(List<AccountMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(resultHandler);
    }

    @Override
    public void storeLiquiGroupMargin(List<LiquiGroupMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(resultHandler);
    }

    @Override
    public void storeLiquiGroupSplitMargin(List<LiquiGroupSplitMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(resultHandler);
    }

    @Override
    public void storePoolMargin(List<PoolMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(resultHandler);
    }

    @Override
    public void storePositionReport(List<PositionReportModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(resultHandler);
    }

    @Override
    public void storeRiskLimitUtilization(List<RiskLimitUtilizationModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(resultHandler);
    }

    @Override
    public void close() {
    }

    private void store(Handler<AsyncResult<Void>> resultHandler) {
        if (++counter <= numberOfFailures) {
            resultHandler.handle(Future.failedFuture(new ServiceException(STORE_ERROR, "Unable to store message")));
        } else {
            this.async.countDown();
            resultHandler.handle(Future.succeededFuture());
        }
    }
}
