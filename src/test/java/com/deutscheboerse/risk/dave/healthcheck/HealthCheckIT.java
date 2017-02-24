package com.deutscheboerse.risk.dave.healthcheck;

import com.deutscheboerse.risk.dave.BaseTest;
import com.deutscheboerse.risk.dave.HealthCheckVerticle;
import com.deutscheboerse.risk.dave.MainVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class HealthCheckIT extends BaseTest {

    private static Vertx vertx;

    @BeforeClass
    public static void setUp(TestContext context) {
        vertx = Vertx.vertx();

        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(BaseTest.getGlobalConfig());
        vertx.deployVerticle(MainVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
    }

    private Handler<HttpClientResponse> assertEqualsHttpHandler(int expectedCode, String expectedText, TestContext context) {
        final Async async = context.async();
        return response -> {
            context.assertEquals(expectedCode, response.statusCode());
            response.bodyHandler(body -> {
                try {
                    context.assertEquals(expectedText, body.toString());
                    async.complete();
                } catch (Exception e) {
                    context.fail(e);
                }
            });
        };
    }

    @Test
    public void testHealth(TestContext context) throws InterruptedException {
        vertx.createHttpClient().getNow(HTTP_PORT, "localhost", HealthCheckVerticle.REST_HEALTHZ,
                assertEqualsHttpHandler(200, "ok", context));
    }

    @Test
    public void testReadinessOk(TestContext context) throws InterruptedException {
        vertx.createHttpClient().getNow(HTTP_PORT, "localhost", HealthCheckVerticle.REST_READINESS,
                assertEqualsHttpHandler(200, "ok", context));
    }

    @Test
    public void testReadinessNok(TestContext context) throws InterruptedException {
        HealthCheck healthCheck = new HealthCheck(vertx);
        healthCheck.setMainState(false);

        vertx.createHttpClient().getNow(HTTP_PORT, "localhost", HealthCheckVerticle.REST_READINESS,
                assertEqualsHttpHandler(503, "nok", context));
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }
}
