package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.model.LiquiGroupSplitMarginModel;
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
public class LiquiGroupSplitMarginVerticleIT {
    private static Vertx vertx;

    @BeforeClass
    public static void setUp(TestContext context) {
        LiquiGroupSplitMarginVerticleIT.vertx = Vertx.vertx();
        final BrokerFiller brokerFiller = new BrokerFiller(LiquiGroupSplitMarginVerticleIT.vertx);
        brokerFiller.setUpLiquiGroupSplitMarginQueue(context.asyncAssertSuccess());
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        LiquiGroupSplitMarginVerticleIT.vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void testPoolMarginVerticle(TestContext context) throws InterruptedException {
        int tcpPort = Integer.getInteger("cil.tcpport", 5672);
        JsonObject config = new JsonObject()
                .put("port", tcpPort)
                .put("listeners", new JsonObject()
                        .put("liquiGroupSplitMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupSplitMargin"));

        // we expect 2472 messages to be received
        Async async = context.async(2472);
        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer("persistenceService");
        LiquiGroupSplitMarginModel liquiGroupSplitMarginModel = new LiquiGroupSplitMarginModel();
        consumer.handler(message -> {
            JsonObject body = message.body();
            liquiGroupSplitMarginModel.clear();
            liquiGroupSplitMarginModel.mergeIn(body.getJsonObject("message"));
            async.countDown();
        });
        vertx.deployVerticle(LiquiGroupSplitMarginVerticle.class.getName(), new DeploymentOptions().setConfig(config), context.asyncAssertSuccess());
        async.awaitSuccess(30000);

        System.out.println(liquiGroupSplitMarginModel.encodePrettily());

        JsonObject expected = new JsonObject()
                .put("snapshotID", 15)
                .put("businessDate", 20091215)
                .put("timestamp", new JsonObject().put("$date", "2017-02-21T11:43:34.791Z"))
                .put("clearer", "USJPM")
                .put("member", "USJPM")
                .put("account", "PP")
                .put("liquidationGroup", "PFI02")
                .put("liquidationGroupSplit", "PFI02_HP2_T3-99999")
                .put("marginCurrency", "EUR")
                .put("premiumMargin", 0.0)
                .put("marketRisk", 2.7548216040760565E8)
                .put("liquRisk", 3.690967426538666E7)
                .put("longOptionCredit", 0.0)
                .put("variationPremiumPayment", 4.86621581017E8);
        context.assertEquals(expected, new JsonObject(liquiGroupSplitMarginModel.getMap()));
    }
}
