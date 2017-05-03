package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck.Component;
import com.deutscheboerse.risk.dave.model.LiquiGroupSplitMarginModel;
import com.google.protobuf.Extension;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.function.BiFunction;

public class LiquiGroupSplitMarginVerticle extends AMQPVerticle<PrismaReports.LiquiGroupSplitMargin, LiquiGroupSplitMarginModel> {
    @Override
    protected Component getHealthCheckComponent() {
        return Component.LIQUI_GROUP_SPLIT_MARGIN;
    }

    @Override
    protected Extension<ObjectList.GPBObject, PrismaReports.LiquiGroupSplitMargin> getGpbExtension() {
        return PrismaReports.liquiGroupSplitMargin;
    }

    @Override
    protected BiFunction<PrismaReports.PrismaHeader, PrismaReports.LiquiGroupSplitMargin, LiquiGroupSplitMarginModel> getModelFactory() {
        return LiquiGroupSplitMarginModel::new;
    }

    @Override
    protected String getAmqpQueueName() {
        return this.getAmqpConfig().getListeners().getLiquiGroupSplitMargin();
    }

    @Override
    protected void store(LiquiGroupSplitMarginModel model, Handler<AsyncResult<Void>> handler) {
        this.getPersistenceService().storeLiquiGroupSplitMargin(model, handler);
    }
}