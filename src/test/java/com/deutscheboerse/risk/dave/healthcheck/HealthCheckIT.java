package com.deutscheboerse.risk.dave.healthcheck;

import com.deutscheboerse.risk.dave.HealthCheckVerticle;
import com.deutscheboerse.risk.dave.MainVerticle;
import com.deutscheboerse.risk.dave.persistence.SuccessPersistenceService;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import com.deutscheboerse.risk.dave.utils.TestConfig;
import com.google.inject.AbstractModule;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class HealthCheckIT {

    private static Vertx vertx;

    @BeforeClass
    public static void setUp(TestContext context) {
        vertx = Vertx.vertx();

        JsonObject config = TestConfig.getGlobalConfig();
        config.put("guice_binder", SuccessBinder.class.getName());
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(config);

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
        JsonObject expected = new JsonObject();
        expected.put("checks", new JsonArray().add(new JsonObject()
                .put("id", "healthz")
                .put("status", "UP")))
                .put("outcome", "UP");
        vertx.createHttpClient().getNow(TestConfig.HEALTHCHECK_PORT, "localhost", HealthCheckVerticle.REST_HEALTHZ,
                assertEqualsHttpHandler(200, expected.encode(), context));
    }

    @Test
    public void testReadinessOk(TestContext context) throws InterruptedException {
        JsonObject expected = new JsonObject();
        expected.put("checks", new JsonArray().add(new JsonObject()
                .put("id", "readiness")
                .put("status", "UP")))
                .put("outcome", "UP");
        vertx.createHttpClient().getNow(TestConfig.HEALTHCHECK_PORT, "localhost", HealthCheckVerticle.REST_READINESS,
                assertEqualsHttpHandler(200, expected.encode(), context));
    }

    @Test
    public void testReadinessNok(TestContext context) throws InterruptedException {
        JsonObject expected = new JsonObject();
        expected.put("checks", new JsonArray().add(new JsonObject()
                .put("id", "readiness")
                .put("status", "DOWN")))
                .put("outcome", "DOWN");

        HealthCheck healthCheck = new HealthCheck(vertx);
        healthCheck.setComponentFailed(HealthCheck.Component.ACCOUNT_MARGIN);

        vertx.createHttpClient().getNow(TestConfig.HEALTHCHECK_PORT, "localhost", HealthCheckVerticle.REST_READINESS,
                assertEqualsHttpHandler(503, expected.encode(), context));
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    public static class SuccessBinder extends AbstractModule {

        @Override
        protected void configure() {
            bind(PersistenceService.class).to(SuccessPersistenceService.class);
        }
    }
}
