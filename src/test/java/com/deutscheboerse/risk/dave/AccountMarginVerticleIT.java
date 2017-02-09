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
        async.awaitSuccess(5000);

        // verify the content of the last message
        AccountMarginModel expectedAccoungMargin = new AccountMarginModel();
        expectedAccoungMargin.setSnapshotID(5);
        expectedAccoungMargin.setBusinessDate(20091215);
        expectedAccoungMargin.setTimestamp(1486465721933L);
        expectedAccoungMargin.setClearer("SFUCC");
        expectedAccoungMargin.setMember("SFUFR");
        expectedAccoungMargin.setAccount("A5");
        expectedAccoungMargin.setMarginCurrency("EUR");
        expectedAccoungMargin.setClearingCurrency("EUR");
        expectedAccoungMargin.setPool("default");
        expectedAccoungMargin.setMarginReqInMarginCurr(5.035485884371926E7);
        expectedAccoungMargin.setMarginReqInCrlCurr(5.035485884371926E7);
        expectedAccoungMargin.setUnadjustedMarginRequirement(5.035485884371926E7);
        expectedAccoungMargin.setVariationPremiumPayment(0.0);
        context.assertEquals(expectedAccoungMargin, accountMargin);
    }
}
