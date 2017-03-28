package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck.Component;
import com.deutscheboerse.risk.dave.model.LiquiGroupSplitMarginModel;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class LiquiGroupSplitMarginVerticle extends AMQPVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(LiquiGroupSplitMarginVerticle.class);

    @Override
    protected String getAmqpContainerName() {
        return "dave/marginloader-LiquiGroupSplitMarginVerticle";
    }

    @Override
    protected String getAmqpQueueName() {
        return config().getJsonObject("listeners", new JsonObject()).getString("liquiGroupSplitMargin");
    }

    @Override
    protected void onConnect() {
        healthCheck.setComponentReady(Component.LIQUI_GROUP_SPLIT_MARGIN);
    }

    @Override
    protected void onDisconnect() {
        healthCheck.setComponentReady(Component.LIQUI_GROUP_SPLIT_MARGIN);
    }

    @Override
    protected void processObjectList(ObjectList.GPBObjectList gpbObjectList) {
        PrismaReports.PrismaHeader header = gpbObjectList.getHeader().getExtension(PrismaReports.prismaHeader);
        gpbObjectList.getItemList().forEach(gpbObject -> {
            if (gpbObject.hasExtension(PrismaReports.liquiGroupSplitMargin)) {
                PrismaReports.LiquiGroupSplitMargin liquiGroupSplitMarginData = gpbObject.getExtension(PrismaReports.liquiGroupSplitMargin);
                try {
                    LiquiGroupSplitMarginModel liquiGroupSplitMarginModel = new LiquiGroupSplitMarginModel(header, liquiGroupSplitMarginData);
                    this.persistenceService.storeLiquiGroupSplitMargin(liquiGroupSplitMarginModel, ar -> {
                        if (ar.succeeded()) {
                            LOG.debug("Liqui Group Split Margin message processed");
                        } else {
                            LOG.error("Unable to store message", ar.cause());
                        }
                    });
                } catch (IllegalArgumentException ex) {
                    LOG.error("Unable to create Liqui Group Split Margin Model from GPB data", ex);
                }
            } else {
                LOG.error("Unknown extension (should be {})", PrismaReports.liquiGroupSplitMargin.getDescriptor().getName());
            }
        });
    }

}