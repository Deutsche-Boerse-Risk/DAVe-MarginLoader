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
        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(LiquiGroupMarginModel.EB_STORE_ADDRESS);
        LiquiGroupMarginModel liquiGroupMarginModel = new LiquiGroupMarginModel();
        consumer.handler(message -> {
            JsonObject body = message.body();
            liquiGroupMarginModel.clear();
            liquiGroupMarginModel.mergeIn(body);
            async.countDown();
        });
        vertx.deployVerticle(LiquiGroupMarginVerticle.class.getName(), new DeploymentOptions().setConfig(config), context.asyncAssertSuccess());
        async.awaitSuccess(30000);

        // verify the content of the last message
        LiquiGroupMarginModel expectedLiquiGroupMarginMargin = new LiquiGroupMarginModel();
        expectedLiquiGroupMarginMargin.setSnapshotID(5);
        expectedLiquiGroupMarginMargin.setBusinessDate(20091215);
        expectedLiquiGroupMarginMargin.setTimestamp(1486465721933L);
        expectedLiquiGroupMarginMargin.setClearer("ABCFR");
        expectedLiquiGroupMarginMargin.setMember("ABCFR");
        expectedLiquiGroupMarginMargin.setAccount("PP");
        expectedLiquiGroupMarginMargin.setMarginClass("ECC01");
        expectedLiquiGroupMarginMargin.setMarginCurrency("EUR");
        expectedLiquiGroupMarginMargin.setMarginGroup("");
        expectedLiquiGroupMarginMargin.setPremiumMargin(135000.5);
        expectedLiquiGroupMarginMargin.setCurrentLiquidatingMargin(0.0);
        expectedLiquiGroupMarginMargin.setFuturesSpreadMargin(0.0);
        expectedLiquiGroupMarginMargin.setAdditionalMargin(14914.841270178167);
        expectedLiquiGroupMarginMargin.setUnadjustedMarginRequirement(149915.34127017818);
        expectedLiquiGroupMarginMargin.setVariationPremiumPayment(0.0);
        context.assertEquals(expectedLiquiGroupMarginMargin, liquiGroupMarginModel);
    }
}
