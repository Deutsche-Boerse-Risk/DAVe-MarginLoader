package com.deutscheboerse.risk.dave;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.deutscheboerse.risk.dave.model.LiquiGroupMarginModel;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;

@RunWith(VertxUnitRunner.class)
public class LiquiGroupMarginVerticleIT {
    private static Vertx vertx;

    @BeforeClass
    public static void setUp(TestContext context) {
        LiquiGroupMarginVerticleIT.vertx = Vertx.vertx();
        final BrokerFiller brokerFiller = new BrokerFiller(LiquiGroupMarginVerticleIT.vertx);
        brokerFiller.setUpLiquiGroupMarginQueue(context.asyncAssertSuccess());
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        LiquiGroupMarginVerticleIT.vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void testLiquiGroupMarginVerticle(TestContext context) throws InterruptedException {
        int tcpPort = Integer.getInteger("cil.tcpport", 5672);
        JsonObject config = new JsonObject()
                .put("port", tcpPort)
                .put("listeners", new JsonObject()
                        .put("liquiGroupMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupMargin"));

        // we expect 2171 messages to be received
        Async async = context.async(2171);
        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer("persistenceService");
        LiquiGroupMarginModel liquiGroupMarginModel = new LiquiGroupMarginModel();
        consumer.handler(message -> {
            JsonObject body = message.body();
            liquiGroupMarginModel.clear();
            liquiGroupMarginModel.mergeIn(body.getJsonObject("message"));
            async.countDown();
        });
        vertx.deployVerticle(LiquiGroupMarginVerticle.class.getName(), new DeploymentOptions().setConfig(config), context.asyncAssertSuccess());
        async.awaitSuccess(30000);

        // verify the content of the last message
        JsonObject expected = new JsonObject()
                .put("snapshotID", 10)
                .put("businessDate", 20091215)
                .put("timestamp", new JsonObject().put("$date", "2017-02-15T15:27:02.43Z"))
                .put("clearer", "ABCFR")
                .put("member", "ABCFR")
                .put("account", "PP")
                .put("marginClass", "ECC01")
                .put("marginCurrency", "EUR")

                .put("marginGroup", "")
                .put("premiumMargin", 135000.5)
                .put("currentLiquidatingMargin", 0.0)
                .put("futuresSpreadMargin", 0.0)
                .put("additionalMargin", 14914.841270178167)
                .put("unadjustedMarginRequirement", 149915.34127017818)
                .put("variationPremiumPayment", 0.0);
        context.assertEquals(expected, new JsonObject(liquiGroupMarginModel.getMap()));
    }
}
