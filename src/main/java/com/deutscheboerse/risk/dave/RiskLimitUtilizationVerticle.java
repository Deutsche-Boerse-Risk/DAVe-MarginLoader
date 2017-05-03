package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import com.deutscheboerse.risk.dave.model.RiskLimitUtilizationModel;
import com.google.protobuf.Extension;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;
import java.util.function.BiFunction;

public class RiskLimitUtilizationVerticle extends AMQPVerticle<PrismaReports.RiskLimitUtilization, RiskLimitUtilizationModel> {
    @Override
    protected HealthCheck.Component getHealthCheckComponent() {
        return HealthCheck.Component.RISK_LIMIT_UTILIZATION;
    }

    @Override
    protected Extension<ObjectList.GPBObject, PrismaReports.RiskLimitUtilization> getGpbExtension() {
        return PrismaReports.riskLimitUtilization;
    }

    @Override
    protected BiFunction<PrismaReports.PrismaHeader, PrismaReports.RiskLimitUtilization, RiskLimitUtilizationModel> getModelFactory() {
        return RiskLimitUtilizationModel::new;
    }

    @Override
    protected String getAmqpQueueName() {
        return this.getAmqpConfig().getListeners().getRiskLimitUtilization();
    }

    @Override
    protected void store(List<RiskLimitUtilizationModel> models, Handler<AsyncResult<Void>> handler) {
        this.getPersistenceService().storeRiskLimitUtilization(models, handler);
    }
}