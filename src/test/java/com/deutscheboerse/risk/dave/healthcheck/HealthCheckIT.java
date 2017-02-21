package com.deutscheboerse.risk.dave.healthcheck;

import com.deutscheboerse.risk.dave.HealthCheckVerticle;
import com.deutscheboerse.risk.dave.MainVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class HealthCheckIT {

    private static final int BROKER_PORT = Integer.getInteger("cil.tcpport", 5672);
    private static final String DB_NAME = "DAVe-Test" + UUID.randomUUID().getLeastSignificantBits();
    private static final int DB_PORT =  Integer.getInteger("mongodb.port", 27017);
    private static final int HTTP_PORT = Integer.getInteger("http.port", 8080);
    private static Vertx vertx;

    @BeforeClass
    public static void setUp(TestContext context) {
        vertx = Vertx.vertx();

        vertx.deployVerticle(MainVerticle.class.getName(), createDeploymentOptions(), context.asyncAssertSuccess());
    }

    private static DeploymentOptions createDeploymentOptions() {
        JsonObject brokerConfig = new JsonObject()
                .put("port", BROKER_PORT)
                .put("listeners", new JsonObject()
                        .put("accountMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEAccountMargin")
                        .put("liquiGroupMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupMargin")
                        .put("liquiGroupSplitMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupSplitMargin")
                        .put("poolMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEPoolMargin"));
        JsonObject mongoConfig = new JsonObject()
                .put("dbName", DB_NAME)
                .put("connectionUrl", String.format("mongodb://localhost:%s/?waitqueuemultiple=%d", DB_PORT, 20000));
        JsonObject healthCheckConfig = new JsonObject()
                .put("port", HTTP_PORT);
        JsonObject config = new JsonObject()
                .put("broker", brokerConfig)
                .put("mongo", mongoConfig)
                .put("healthCheck", healthCheckConfig);

        return new DeploymentOptions().setConfig(config);
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
