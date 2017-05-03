package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck.Component;
import com.deutscheboerse.risk.dave.model.LiquiGroupMarginModel;
import com.google.protobuf.Extension;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;
import java.util.function.BiFunction;

public class LiquiGroupMarginVerticle extends AMQPVerticle<PrismaReports.LiquiGroupMargin, LiquiGroupMarginModel> {
    @Override
    protected Component getHealthCheckComponent() {
        return Component.LIQUI_GROUP_MARGIN;
    }

    @Override
    protected Extension<ObjectList.GPBObject, PrismaReports.LiquiGroupMargin> getGpbExtension() {
        return PrismaReports.liquiGroupMargin;
    }

    @Override
    protected BiFunction<PrismaReports.PrismaHeader, PrismaReports.LiquiGroupMargin, LiquiGroupMarginModel> getModelFactory() {
        return LiquiGroupMarginModel::new;
    }

    @Override
    protected String getAmqpQueueName() {
        return this.getAmqpConfig().getListeners().getLiquiGroupMargin();
    }

    @Override
    protected void store(List<LiquiGroupMarginModel> models, Handler<AsyncResult<Void>> handler) {
        this.getPersistenceService().storeLiquiGroupMargin(models, handler);
    }
}
