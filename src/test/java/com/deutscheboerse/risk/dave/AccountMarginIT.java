package com.deutscheboerse.risk.dave;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class AccountMarginIT extends BaseIT {

    @Test
    public void testCalendarVerticle(TestContext context) throws InterruptedException {
        int tcpPort = Integer.getInteger("cil.tcpport", 5672);
        JsonObject config = new JsonObject().put("broker", new JsonObject().put("port", tcpPort).put("listeners", new JsonObject().put("accountMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEAccountMargin")));
        this.testVerticle(AccountMargin.class, config, context);
    }
}
