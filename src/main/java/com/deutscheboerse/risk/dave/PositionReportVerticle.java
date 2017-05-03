package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import com.deutscheboerse.risk.dave.model.PositionReportModel;
import com.google.protobuf.Extension;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;
import java.util.function.BiFunction;

public class PositionReportVerticle extends AMQPVerticle<PrismaReports.PositionReport, PositionReportModel> {
    @Override
    protected HealthCheck.Component getHealthCheckComponent() {
        return HealthCheck.Component.POSITION_REPORT;
    }

    @Override
    protected Extension<ObjectList.GPBObject, PrismaReports.PositionReport> getGpbExtension() {
        return PrismaReports.positionReport;
    }

    @Override
    protected BiFunction<PrismaReports.PrismaHeader, PrismaReports.PositionReport, PositionReportModel> getModelFactory() {
        return PositionReportModel::new;
    }

    @Override
    protected String getAmqpQueueName() {
        return this.getAmqpConfig().getListeners().getPositionReport();
    }

    @Override
    protected void store(List<PositionReportModel> models, Handler<AsyncResult<Void>> handler) {
        this.getPersistenceService().storePositionReport(models, handler);
    }
}