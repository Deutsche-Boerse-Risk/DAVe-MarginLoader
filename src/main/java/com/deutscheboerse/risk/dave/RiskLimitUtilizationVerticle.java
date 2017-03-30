package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck.Component;
import com.deutscheboerse.risk.dave.model.RiskLimitUtilizationModel;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class RiskLimitUtilizationVerticle extends AMQPVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(RiskLimitUtilizationVerticle.class);

    @Override
    protected void onConnect() {
        healthCheck.setComponentReady(Component.RISK_LIMIT_UTILIZATION);
    }

    @Override
    protected void onDisconnect() {
        healthCheck.setComponentFailed(Component.RISK_LIMIT_UTILIZATION);
    }

    @Override
    protected void processObjectList(ObjectList.GPBObjectList gpbObjectList) {
        PrismaReports.PrismaHeader header = gpbObjectList.getHeader().getExtension(PrismaReports.prismaHeader);
        gpbObjectList.getItemList().forEach(gpbObject -> {
            if (gpbObject.hasExtension(PrismaReports.riskLimitUtilization)) {
                PrismaReports.RiskLimitUtilization data = gpbObject.getExtension(PrismaReports.riskLimitUtilization);
                try {
                    RiskLimitUtilizationModel model = new RiskLimitUtilizationModel(header, data);
                    this.persistenceService.storeRiskLimitUtilization(model, ar -> {
                        if (ar.succeeded()) {
                            LOG.debug("Risk Limit Utilization message processed");
                        } else {
                            LOG.error("Unable to store message", ar.cause());
                        }
                    });
                } catch (IllegalArgumentException ex) {
                    LOG.error("Unable to create Risk Limit Utilization Model from GPB data", ex);
                }
            } else {
                LOG.error("Unknown extension (should be {})", PrismaReports.riskLimitUtilization.getDescriptor().getName());
            }
        });
    }

}