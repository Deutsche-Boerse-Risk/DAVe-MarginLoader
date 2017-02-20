package com.deutscheboerse.risk.dave;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.deutscheboerse.risk.dave.model.AccountMarginModel;
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
public class AccountMarginVerticleIT {
    private static Vertx vertx;
    
    @BeforeClass
    public static void setUp(TestContext context) {
        AccountMarginVerticleIT.vertx = Vertx.vertx();
        final BrokerFiller brokerFiller = new BrokerFiller(AccountMarginVerticleIT.vertx);
        brokerFiller.setUpAccountMarginQueue(context.asyncAssertSuccess());
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        AccountMarginVerticleIT.vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void testAccountMarginVerticle(TestContext context) throws InterruptedException {
        int tcpPort = Integer.getInteger("cil.tcpport", 5672);
        JsonObject config = new JsonObject()
                .put("port", tcpPort)
                .put("listeners", new JsonObject()
                        .put("accountMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEAccountMargin"));

        // we expect 1704 messages to be received
        Async async = context.async(1704);
        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer("persistenceService");
        AccountMarginModel accountMargin = new AccountMarginModel();
        consumer.handler(message -> {
            JsonObject body = message.body();
            accountMargin.clear();
            accountMargin.mergeIn(body.getJsonObject("message"));
            async.countDown();
        });
        vertx.deployVerticle(AccountMarginVerticle.class.getName(), new DeploymentOptions().setConfig(config), context.asyncAssertSuccess());
        async.awaitSuccess(30000);

        // verify the content of the last message
        AccountMarginModel expectedAccountMargin = new AccountMarginModel();
        expectedAccountMargin.setSnapshotID(5);
        expectedAccountMargin.setBusinessDate(20091215);
        expectedAccountMargin.setTimestamp(1486465721933L);
        expectedAccountMargin.setClearer("SFUCC");
        expectedAccountMargin.setMember("SFUFR");
        expectedAccountMargin.setAccount("A5");
        expectedAccountMargin.setMarginCurrency("EUR");
        expectedAccountMargin.setClearingCurrency("EUR");
        expectedAccountMargin.setPool("default");
        expectedAccountMargin.setMarginReqInMarginCurr(5.035485884371926E7);
        expectedAccountMargin.setMarginReqInCrlCurr(5.035485884371926E7);
        expectedAccountMargin.setUnadjustedMarginRequirement(5.035485884371926E7);
        expectedAccountMargin.setVariationPremiumPayment(0.0);
        context.assertEquals(expectedAccountMargin, accountMargin);
    }
}
