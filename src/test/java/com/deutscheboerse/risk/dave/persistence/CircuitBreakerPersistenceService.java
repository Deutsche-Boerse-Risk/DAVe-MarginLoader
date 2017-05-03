package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.model.*;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.unit.Async;
import io.vertx.serviceproxy.ServiceException;

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
    public void storeAccountMargin(AccountMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(resultHandler);
    }

    @Override
    public void storeLiquiGroupMargin(LiquiGroupMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(resultHandler);
    }

    @Override
    public void storeLiquiGroupSplitMargin(LiquiGroupSplitMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(resultHandler);
    }

    @Override
    public void storePoolMargin(PoolMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(resultHandler);
    }

    @Override
    public void storePositionReport(PositionReportModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(resultHandler);
    }

    @Override
    public void storeRiskLimitUtilization(RiskLimitUtilizationModel model, Handler<AsyncResult<Void>> resultHandler) {
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
