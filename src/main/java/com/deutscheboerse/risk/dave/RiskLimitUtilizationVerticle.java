package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.model.ModelType;
import com.deutscheboerse.risk.dave.model.RiskLimitUtilizationModel;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class RiskLimitUtilizationVerticle extends AMQPVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(RiskLimitUtilizationVerticle.class);

    @Override
    public void start(Future<Void> fut) throws Exception {
        super.start(fut, RiskLimitUtilizationVerticle.class.getSimpleName());
    }

    @Override
    protected String getAmqpContainerName() {
        return "dave/marginloader-RiskLimitUtilizationVerticle";
    }

    @Override
    protected String getAmqpQueueName() {
        return config().getJsonObject("listeners", new JsonObject()).getString("riskLimitUtilization");
    }

    @Override
    protected void processObjectList(ObjectList.GPBObjectList gpbObjectList) {
        PrismaReports.PrismaHeader header = gpbObjectList.getHeader().getExtension(PrismaReports.prismaHeader);
        gpbObjectList.getItemList().forEach(gpbObject -> {
            if (gpbObject.hasExtension(PrismaReports.riskLimitUtilization)) {
                PrismaReports.RiskLimitUtilization data = gpbObject.getExtension(PrismaReports.riskLimitUtilization);
                try {
                    RiskLimitUtilizationModel model = new RiskLimitUtilizationModel(header, data);
                    this.persistenceService.store(model, ModelType.RISK_LIMIT_UTILIZATION_MODEL, ar -> {
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