package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import com.deutscheboerse.risk.dave.persistence.CountdownPersistenceService;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.serviceproxy.ProxyHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;

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

        // Setup persistence persistence
        CountdownPersistenceService persistenceService = new CountdownPersistenceService(vertx, async);
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        vertx.deployVerticle(AccountMarginVerticle.class.getName(), new DeploymentOptions().setConfig(config), context.asyncAssertSuccess());
        async.awaitSuccess(30000);

        // verify the content of the last message
        JsonObject expected = new JsonObject()
                .put("snapshotID", 10)
                .put("businessDate", 20091215)
                .put("timestamp", new JsonObject().put("$date", "2017-02-15T15:27:02.43Z"))
                .put("clearer", "SFUCC")
                .put("member", "SFUFR")
                .put("account", "A5")
                .put("marginCurrency", "EUR")
                .put("clearingCurrency", "EUR")
                .put("pool", "default")
                .put("marginReqInMarginCurr", 5.035485884371926E7)
                .put("marginReqInCrlCurr", 5.035485884371926E7)
                .put("unadjustedMarginRequirement", 5.035485884371926E7)
                .put("variationPremiumPayment", 0.0);
        context.assertEquals(expected, persistenceService.getLastMessage());

        ProxyHelper.unregisterService(serviceMessageConsumer);
    }
}
