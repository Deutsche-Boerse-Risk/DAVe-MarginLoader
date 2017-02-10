package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.model.AccountMarginModel;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class AccountMarginVerticle extends AMQPVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(AccountMarginVerticle.class);

    @Override
    public void start(Future<Void> fut) throws Exception {
        LOG.info("Starting {} with configuration: {}", AccountMarginVerticle.class.getSimpleName(), config().encodePrettily());
        Future<Void> startFuture = Future.future();
        startFuture.setHandler(ar -> {
            if (ar.succeeded()) {
                LOG.info("{} started", AccountMarginVerticle.class.getSimpleName());
                fut.complete();
            } else {
                LOG.error("{} verticle failed to deploy", AccountMarginVerticle.class.getSimpleName(), fut.cause());
                fut.fail(fut.cause());
            }
        });
        super.start(startFuture);
    }

    @Override
    protected String getAmqpContainerName() {
        return "dave/marginloader-AccountMarginVerticle";
    }

    @Override
    protected String getAmqpQueueName() {
        return config().getJsonObject("listeners", new JsonObject()).getString("accountMargin");
    }

    @Override
    protected void processObjectList(ObjectList.GPBObjectList gpbObjectList) {
        PrismaReports.PrismaHeader header = gpbObjectList.getHeader().getExtension(PrismaReports.prismaHeader);
        gpbObjectList.getItemList().forEach(gpbObject -> {
            if (gpbObject.hasExtension(PrismaReports.accountMargin)) {
                PrismaReports.AccountMargin accountMarginData = gpbObject.getExtension(PrismaReports.accountMargin);
                try {
                    AccountMarginModel accountMarginModel = new AccountMarginModel(header, accountMarginData);
                    vertx.eventBus().send(AccountMarginModel.EB_STORE_ADDRESS, accountMarginModel);
                    LOG.debug("Account Margin message processed");
                } catch (IllegalArgumentException ex) {
                    LOG.error("Unable to create Account Margin Model from GPB data", ex);
                }
            } else {
                LOG.error("Unknown extension (should be {})", PrismaReports.accountMargin);
            }
        });
    }

}