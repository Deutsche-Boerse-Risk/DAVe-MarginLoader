package com.deutscheboerse.risk.dave.healthcheck;

import io.vertx.core.Vertx;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class HealthCheckTest {

    private final static String MAP_NAME = "healthCheck";
    private final static String MAIN_KEY = "mainReady";

    @Test
    public void testInitialization(TestContext context) {
        Vertx vertx = Vertx.vertx();

        HealthCheck healthCheck = new HealthCheck(vertx).initialize();
        LocalMap<String, Boolean> localMap = vertx.sharedData().getLocalMap(MAP_NAME);

        context.assertNotNull(localMap.get(MAIN_KEY), "MAIN readiness should not be null");
        context.assertFalse(localMap.get(MAIN_KEY), "MAIN readiness should be initialized to false");
        context.assertFalse(healthCheck.getMainState(), "MAIN readiness should return false");

        vertx.close();
    }

    @Test
    public void testReadiness(TestContext context) {
        Vertx vertx;

        vertx = Vertx.vertx();
        context.assertFalse(new HealthCheck(vertx).initialize().ready(), "Nothing is ready, should return false");
        vertx.close();

        vertx = Vertx.vertx();
        context.assertTrue(new HealthCheck(vertx).initialize().setMainState(true).getMainState(), "Main verticle is ready");
        vertx.close();

        vertx = Vertx.vertx();
        context.assertTrue(new HealthCheck(vertx).initialize().setMainState(true).ready(), "Main verticle is ready, the whole app should be ready");
        vertx.close();
    }
}
