package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.utils.TestConfig;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

@RunWith(VertxUnitRunner.class)
public class HealthCheckVerticleTest {
    private static Vertx vertx;

    @BeforeClass
    public static void setUp(TestContext context) throws IOException {
        vertx = Vertx.vertx();
        DeploymentOptions options = new DeploymentOptions().setConfig(TestConfig.getHealthCheckConfig());
        vertx.deployVerticle(HealthCheckVerticle.class.getName(), options, context.asyncAssertSuccess());
    }

    @Test
    public void testPlainHttp(TestContext context) {
        final Async asyncClient = context.async();

        vertx.createHttpClient().getNow(TestConfig.HEALTHCHECK_PORT, "localhost", "/healthz", res -> {
            context.assertEquals(200, res.statusCode());
            asyncClient.complete();
        });
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

}
