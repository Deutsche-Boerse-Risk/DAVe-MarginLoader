package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.model.PoolMarginModel;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class PoolMarginVerticle extends AMQPVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(PoolMarginVerticle.class);

    @Override
    public void start(Future<Void> fut) throws Exception {
        super.start(fut, PoolMarginVerticle.class.getSimpleName());
    }

    @Override
    protected String getAmqpContainerName() {
        return "dave/marginloader-PoolMarginVerticle";
    }

    @Override
    protected String getAmqpQueueName() {
        return config().getJsonObject("listeners", new JsonObject()).getString("poolMargin");
    }

    @Override
    protected void onConnect() {
        healthCheck.setPoolMarginState(true);
    }

    @Override
    protected void onDisconnect() {
        healthCheck.setPoolMarginState(false);
    }

    @Override
    protected void processObjectList(ObjectList.GPBObjectList gpbObjectList) {
        PrismaReports.PrismaHeader header = gpbObjectList.getHeader().getExtension(PrismaReports.prismaHeader);
        gpbObjectList.getItemList().forEach(gpbObject -> {
            if (gpbObject.hasExtension(PrismaReports.poolMargin)) {
                PrismaReports.PoolMargin poolMarginData = gpbObject.getExtension(PrismaReports.poolMargin);
                try {
                    PoolMarginModel poolMarginModel = new PoolMarginModel(header, poolMarginData);
                    this.persistenceService.storePoolMargin(poolMarginModel, ar -> {
                        if (ar.succeeded()) {
                            LOG.debug("Pool Margin message processed");
                        } else {
                            LOG.error("Unable to store message", ar.cause());
                        }
                    });
                } catch (IllegalArgumentException ex) {
                    LOG.error("Unable to create Pool Margin Model from GPB data", ex);
                }
            } else {
                LOG.error("Unknown extension (should be {})", PrismaReports.poolMargin.getDescriptor().getName());
            }
        });
    }

}