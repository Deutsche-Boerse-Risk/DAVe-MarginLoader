package com.deutscheboerse.risk.dave;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class BaseIT {
    protected static Vertx vertx;

    @BeforeClass
    public static void setUp(TestContext context) {
        BaseIT.vertx = Vertx.vertx();
        final BrokerFiller brokerFiller = new BrokerFiller(BaseIT.vertx);
        final Async async = context.async();
        Future<?> brokerFillerFuture = brokerFiller.setUp();
        brokerFillerFuture.setHandler(ar -> {
            if (ar.succeeded()) {
                async.complete();
            } else {
                context.fail(brokerFillerFuture.cause());
            }
        });
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        BaseIT.vertx.close(context.asyncAssertSuccess());
    }

    protected void deployVerticle(Vertx vertx, TestContext context, Class<?> clazz, JsonObject config) {
        final Async asyncStart = context.async();
        vertx.deployVerticle(clazz.getName(), new DeploymentOptions().setConfig(config), res -> {
            if (res.succeeded()) {
                asyncStart.complete();
            } else {
                context.fail(res.cause());
            }
        });
        asyncStart.awaitSuccess();
    }

    protected void testVerticle(Class<?> clazz, JsonObject config, TestContext context) throws InterruptedException {
        this.deployVerticle(BaseIT.vertx, context, clazz, config);
    }

}
