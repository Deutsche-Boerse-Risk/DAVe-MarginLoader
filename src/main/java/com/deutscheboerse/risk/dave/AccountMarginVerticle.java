package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import com.deutscheboerse.risk.dave.model.AccountMarginModel;
import com.google.protobuf.Extension;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;
import java.util.function.BiFunction;

public class AccountMarginVerticle extends AMQPVerticle<PrismaReports.AccountMargin, AccountMarginModel> {
    @Override
    protected HealthCheck.Component getHealthCheckComponent() {
        return HealthCheck.Component.ACCOUNT_MARGIN;
    }

    @Override
    protected Extension<ObjectList.GPBObject, PrismaReports.AccountMargin> getGpbExtension() {
        return PrismaReports.accountMargin;
    }

    @Override
    protected BiFunction<PrismaReports.PrismaHeader, PrismaReports.AccountMargin, AccountMarginModel> getModelFactory() {
        return AccountMarginModel::new;
    }

    @Override
    protected String getAmqpQueueName() {
        return this.getAmqpConfig().getListeners().getAccountMargin();
    }

    @Override
    protected void store(List<AccountMarginModel> models, Handler<AsyncResult<Void>> handler) {
        this.getPersistenceService().storeAccountMargin(models, handler);
    }
}
