package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck.Component;
import com.deutscheboerse.risk.dave.model.PoolMarginModel;
import com.google.protobuf.Extension;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.function.BiFunction;

public class PoolMarginVerticle extends AMQPVerticle<PrismaReports.PoolMargin, PoolMarginModel> {
    @Override
    protected Component getHealthCheckComponent() {
        return Component.POOL_MARGIN;
    }

    @Override
    protected Extension<ObjectList.GPBObject, PrismaReports.PoolMargin> getGpbExtension() {
        return PrismaReports.poolMargin;
    }

    @Override
    protected BiFunction<PrismaReports.PrismaHeader, PrismaReports.PoolMargin, PoolMarginModel> getModelFactory() {
        return PoolMarginModel::new;
    }

    @Override
    protected String getAmqpQueueName() {
        return this.getAmqpConfig().getListeners().getPoolMargin();
    }

    @Override
    protected void store(PoolMarginModel model, Handler<AsyncResult<Void>> handler) {
        this.getPersistenceService().storePoolMargin(model, handler);
    }
}