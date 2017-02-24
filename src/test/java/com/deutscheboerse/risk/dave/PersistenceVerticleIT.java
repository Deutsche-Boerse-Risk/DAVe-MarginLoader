package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.persistence.CountdownPersistenceService;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.serviceproxy.ProxyHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class PersistenceVerticleIT {
    private static Vertx vertx;

    @BeforeClass
    public static void setUp(TestContext context) {
        PersistenceVerticleIT.vertx = Vertx.vertx();
    }

    @Test
    public void checkPersistenceServiceInitialized(TestContext context) {
//        Async async = context.async();
//        CountdownPersistenceService persistenceService = new CountdownPersistenceService(vertx, async);
//        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);
//        DeploymentOptions options = new DeploymentOptions().setConfig(new JsonObject());
//        PersistenceVerticleIT.vertx.deployVerticle(PersistenceVerticle.class.getName(), options, context.asyncAssertSuccess());
//        context.assertTrue(persistenceService.isInitialized());
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        PersistenceVerticleIT.vertx.close(context.asyncAssertSuccess());
    }
}
