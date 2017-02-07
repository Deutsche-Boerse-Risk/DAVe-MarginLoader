package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class AccountMargin extends AMQPVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(AccountMargin.class);

    @Override
    public void start(Future<Void> fut) throws Exception {
        LOG.info("Starting {} with configuration: {}", AccountMargin.class.getSimpleName(), config().encodePrettily());
        super.start(fut);
        fut.setHandler(ar -> {
            if (ar.succeeded()) {
                LOG.info("{} started", AccountMargin.class.getSimpleName());
                fut.complete();
            } else {
                LOG.error("{} verticle failed to deploy", AccountMargin.class.getSimpleName(), fut.cause());
                fut.fail(fut.cause());
            }
        });
    }

    @Override
    protected void registerExtensions() {
        PrismaReports.registerAllExtensions(this.registry);
    }

    @Override
    protected String getAmqpContainerName() {
        return "dave/marginloader-AccountMargin";
    }

    @Override
    protected String getAmqpQueueName() {
        return config().getJsonObject("broker", new JsonObject()).getJsonObject("listeners", new JsonObject()).getString("accountMargin");
    }

    @Override
    protected void processObjectList(ObjectList.GPBObjectList gpbObjectList, Handler<AsyncResult<Void>> asyncResult) {
        PrismaReports.PrismaHeader header = gpbObjectList.getHeader().getExtension(PrismaReports.prismaHeader);
        int businessDate = header.getBusinessDate();
        AtomicBoolean allExtensionsProcessed = new AtomicBoolean(true);
        gpbObjectList.getItemList().forEach(gpbObject -> {
            if (gpbObject.hasExtension(PrismaReports.accountMargin)) {
                PrismaReports.AccountMargin accountMarginData = gpbObject.getExtension(PrismaReports.accountMargin);
                LOG.debug("Calendar message processed");
            } else {
                allExtensionsProcessed.set(false);
            }
        });
        if (allExtensionsProcessed.get() == true) {
            LOG.info("All GPB extensions from AMQP message processed");
            asyncResult.handle(Future.succeededFuture());
        } else {
            asyncResult.handle(Future.failedFuture("Unknown data extension"));
        }
    }

}