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
        context.assertEquals(5, accountMargin.getSnapshotID());
        context.assertEquals(20091215, accountMargin.getBusinessDate());
        context.assertEquals(new JsonObject().put("$date", "2017-02-07T11:08:41.933Z"), accountMargin.getTimestamp());
        context.assertEquals("SFUCC", accountMargin.getClearer());
        context.assertEquals("SFUFR", accountMargin.getMember());
        context.assertEquals("A5", accountMargin.getAccount());
        context.assertEquals("EUR", accountMargin.getMarginCurrency());
        context.assertEquals("EUR", accountMargin.getClearingCurrency());
        context.assertEquals("default", accountMargin.getPool());
        context.assertEquals(5.035485884371926E7, accountMargin.getMarginReqInMarginCurr());
        context.assertEquals(5.035485884371926E7, accountMargin.getMarginReqInCrlCurr());
        context.assertEquals(5.035485884371926E7, accountMargin.getUnadjustedMarginRequirement());
        context.assertEquals(0.0, accountMargin.getVariationPremiumPayment());
    }
}
