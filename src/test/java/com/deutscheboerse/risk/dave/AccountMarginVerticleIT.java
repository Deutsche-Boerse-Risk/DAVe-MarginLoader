package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.model.AccountMarginModel;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class AccountMarginVerticleIT extends BaseIT {

    @Test
    public void testAccountMarginVerticle(TestContext context) throws InterruptedException {
        int tcpPort = Integer.getInteger("cil.tcpport", 5672);
        JsonObject config = new JsonObject()
                .put("port", tcpPort)
                .put("listeners", new JsonObject()
                        .put("accountMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEAccountMargin"));
        this.deployVerticle(vertx, context, AccountMarginVerticle.class, config);

        // we expect 1704 messages to be received
        Async async = context.async(1704);
        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(AccountMarginModel.EB_STORE_ADDRESS);
        AccountMarginModel accountMargin = new AccountMarginModel();
        consumer.handler(message -> {
            JsonObject body = message.body();
            accountMargin.clear();
            accountMargin.mergeIn(body);
            async.countDown();
        });
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
