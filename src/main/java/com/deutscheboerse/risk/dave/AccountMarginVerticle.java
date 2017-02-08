package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.model.AccountMarginModel;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class AccountMarginVerticle extends AMQPVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(AccountMarginVerticle.class);

    @Override
    public void start(Future<Void> fut) throws Exception {
        LOG.info("Starting {} with configuration: {}", AccountMarginVerticle.class.getSimpleName(), config().encodePrettily());
        super.start(fut);
        fut.setHandler(ar -> {
            if (ar.succeeded()) {
                LOG.info("{} started", AccountMarginVerticle.class.getSimpleName());
                fut.complete();
            } else {
                LOG.error("{} verticle failed to deploy", AccountMarginVerticle.class.getSimpleName(), fut.cause());
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
        return "dave/marginloader-AccountMarginVerticle";
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
                AccountMarginModel accountMarginModel = new AccountMarginModel(header, accountMarginData);
                vertx.eventBus().send(AccountMarginModel.EB_STORE_ADDRESS, accountMarginModel);
                LOG.debug("Account Margin message processed");
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