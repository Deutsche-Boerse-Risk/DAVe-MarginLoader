package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.model.LiquiGroupSplitMarginModel;
import com.deutscheboerse.risk.dave.model.ModelType;
import com.deutscheboerse.risk.dave.model.PoolMarginModel;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class LiquiGroupSplitMarginVerticle extends AMQPVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(LiquiGroupSplitMarginVerticle.class);

    @Override
    public void start(Future<Void> fut) throws Exception {
        super.start(fut, LiquiGroupSplitMarginVerticle.class.getSimpleName());
    }

    @Override
    protected String getAmqpContainerName() {
        return "dave/marginloader-LiquiGroupSplitMarginVerticle";
    }

    @Override
    protected String getAmqpQueueName() {
        return config().getJsonObject("listeners", new JsonObject()).getString("liquiGroupSplitMargin");
    }

    @Override
    protected void processObjectList(ObjectList.GPBObjectList gpbObjectList) {
        PrismaReports.PrismaHeader header = gpbObjectList.getHeader().getExtension(PrismaReports.prismaHeader);
        gpbObjectList.getItemList().forEach(gpbObject -> {
            if (gpbObject.hasExtension(PrismaReports.liquiGroupSplitMargin)) {
                PrismaReports.LiquiGroupSplitMargin liquiGroupSplitMarginData = gpbObject.getExtension(PrismaReports.liquiGroupSplitMargin);
                try {
                    LiquiGroupSplitMarginModel liquiGroupSplitMarginModel = new LiquiGroupSplitMarginModel(header, liquiGroupSplitMarginData);
                    this.persistenceService.store(liquiGroupSplitMarginModel, ModelType.LIQUI_GROUP_SPLIT_MARGIN_MODEL, ar -> {
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