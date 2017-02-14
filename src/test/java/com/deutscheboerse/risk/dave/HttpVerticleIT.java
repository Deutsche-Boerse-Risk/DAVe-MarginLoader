package com.deutscheboerse.risk.dave;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

@RunWith(VertxUnitRunner.class)
public class HttpVerticleIT {

    private static final int HTTP_PORT = Integer.getInteger("http.port", 8080);
    private static Vertx vertx;

    @BeforeClass
    public static void setUp(TestContext context) throws IOException {
        vertx = Vertx.vertx();

        JsonObject config = new JsonObject().put("port", HTTP_PORT);
        DeploymentOptions options = new DeploymentOptions().setConfig(config);

        vertx.deployVerticle(HttpVerticle.class.getName(), options, context.asyncAssertSuccess());
    }

    @Test
    public void testPlainHttp(TestContext context) {
        final Async asyncClient = context.async();

        vertx.createHttpClient().getNow(HTTP_PORT, "localhost", "/healthz", res -> {
            context.assertEquals(200, res.statusCode());
            res.headers().forEach(v -> System.out.println("Headers: " + v));
            asyncClient.complete();
        });
    }

    @After
    public void cleanup(TestContext context)
    {
        vertx.deploymentIDs().forEach(id -> vertx.undeploy(id, context.asyncAssertSuccess()));
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

}
