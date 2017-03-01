package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.model.LiquiGroupMarginModel;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class LiquiGroupMarginVerticle extends AMQPVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(LiquiGroupMarginVerticle.class);

    @Override
    public void start(Future<Void> fut) throws Exception {
        super.start(fut, LiquiGroupMarginVerticle.class.getSimpleName());
    }

    @Override
    protected String getAmqpContainerName() {
        return "dave/marginloader-LiquiGroupMarginVerticle";
    }

    @Override
    protected String getAmqpQueueName() {
        return config().getJsonObject("listeners", new JsonObject()).getString("liquiGroupMargin");
    }

    @Override
    protected void processObjectList(ObjectList.GPBObjectList gpbObjectList) {
        PrismaReports.PrismaHeader header = gpbObjectList.getHeader().getExtension(PrismaReports.prismaHeader);
        gpbObjectList.getItemList().forEach(gpbObject -> {
            if (gpbObject.hasExtension(PrismaReports.liquiGroupMargin)) {
                PrismaReports.LiquiGroupMargin liquiGroupMarginData = gpbObject.getExtension(PrismaReports.liquiGroupMargin);
                try {
                    LiquiGroupMarginModel liquiGroupMarginModel = new LiquiGroupMarginModel(header, liquiGroupMarginData);
                    this.persistenceService.storeLiquiGroupMargin(liquiGroupMarginModel, ar -> {
                        if (ar.succeeded()) {
                            LOG.debug("Liqui Group Margin message processed");
                        } else {
                            LOG.error("Unable to store message", ar.cause());
                        }
                    });
                } catch (IllegalArgumentException ex) {
                    LOG.error("Unable to create Liqui Group Margin Model from GPB data", ex);
                }
            } else {
                LOG.error("Unknown extension (should be {})", PrismaReports.liquiGroupMargin.getDescriptor().getName());
            }
        });
    }

}