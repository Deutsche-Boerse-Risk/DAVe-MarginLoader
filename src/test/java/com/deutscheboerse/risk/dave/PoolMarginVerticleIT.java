package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.model.PoolMarginModel;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class PoolMarginVerticleIT {
    private static Vertx vertx;

    @BeforeClass
    public static void setUp(TestContext context) {
        PoolMarginVerticleIT.vertx = Vertx.vertx();
        final BrokerFiller brokerFiller = new BrokerFiller(PoolMarginVerticleIT.vertx);
        brokerFiller.setUpPoolMarginQueue(context.asyncAssertSuccess());
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        PoolMarginVerticleIT.vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void testPoolMarginVerticle(TestContext context) throws InterruptedException {
        int tcpPort = Integer.getInteger("cil.tcpport", 5672);
        JsonObject config = new JsonObject()
                .put("port", tcpPort)
                .put("listeners", new JsonObject()
                        .put("poolMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEPoolMargin"));

        // we expect 540 messages to be received
        Async async = context.async(270);
        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer("persistenceService");
        PoolMarginModel poolMarginModel = new PoolMarginModel();
        consumer.handler(message -> {
            JsonObject body = message.body();
            poolMarginModel.clear();
            poolMarginModel.mergeIn(body.getJsonObject("message"));
            async.countDown();
        });
        vertx.deployVerticle(PoolMarginVerticle.class.getName(), new DeploymentOptions().setConfig(config), context.asyncAssertSuccess());
        async.awaitSuccess(30000);

        JsonObject expected = new JsonObject()
                .put("snapshotID", 10)
                .put("businessDate", 20091215)
                .put("timestamp", new JsonObject().put("$date", "2017-02-15T15:27:02.43Z"))
                .put("clearer", "CBKFR")
                .put("pool", "default")
                .put("marginCurrency", "CHF")
                .put("clrRptCurrency", "EUR")
                .put("requiredMargin", 0.0)
                .put("cashCollateralAmount", 920294764.124)
                .put("adjustedSecurities", 0.0)
                .put("adjustedGuarantee", 0.0)
                .put("overUnderInMarginCurr", 920294764.124)
                .put("overUnderInClrRptCurr", 688690802.130428)
                .put("variPremInMarginCurr", 920294764.124)
                .put("adjustedExchangeRate", 0.748337194753)
                .put("poolOwner", "CBKFR");
        context.assertEquals(expected, new JsonObject(poolMarginModel.getMap()));
    }
}
