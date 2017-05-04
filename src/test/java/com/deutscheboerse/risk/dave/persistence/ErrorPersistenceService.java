package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.model.*;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.serviceproxy.ServiceException;

import java.util.List;

public class ErrorPersistenceService implements PersistenceService {

    @Override
    public void initialize(Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void storeAccountMargin(List<AccountMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }

    @Override
    public void storeLiquiGroupMargin(List<LiquiGroupMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }

    @Override
    public void storeLiquiGroupSplitMargin(List<LiquiGroupSplitMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }

    @Override
    public void storePoolMargin(List<PoolMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }

    @Override
    public void storePositionReport(List<PositionReportModel> models, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }

    @Override
    public void storeRiskLimitUtilization(List<RiskLimitUtilizationModel> models, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }

    @Override
    public void close() {
    }
}
