package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.model.AccountMarginModel;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class MainVerticleIT {
    private static final int BROKER_PORT = Integer.getInteger("cil.tcpport", 5672);
    private static final String DB_NAME = "DAVe-Test" + UUID.randomUUID().getLeastSignificantBits();
    private static final int DB_PORT =  Integer.getInteger("mongodb.port", 27017);
    private static Vertx vertx;
    private static MongoClient mongoClient;

    @BeforeClass
    public static void setUp(TestContext context) {
        MainVerticleIT.vertx = Vertx.vertx();
        JsonObject brokerConfig = new JsonObject()
                .put("port", BROKER_PORT)
                .put("listeners", new JsonObject()
                        .put("accountMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEAccountMargin"));
        JsonObject mongoConfig = new JsonObject()
                .put("dbName", MainVerticleIT.DB_NAME)
                .put("connectionUrl", "mongodb://localhost:" + MainVerticleIT.DB_PORT);
        JsonObject config = new JsonObject()
                .put("broker", brokerConfig)
                .put("mongo", mongoConfig);
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        MainVerticleIT.vertx.deployVerticle(MainVerticle.class.getName(), options, context.asyncAssertSuccess());
        MainVerticleIT.createMongoClient();
    }

    private static void createMongoClient() {
        JsonObject mongoConfig = new JsonObject()
                .put("db_name", MainVerticleIT.DB_NAME)
                .put("connection_string", "mongodb://localhost:" + MainVerticleIT.DB_PORT);
        MainVerticleIT.mongoClient = MongoClient.createShared(MainVerticleIT.vertx, mongoConfig);
    }

    @Test
    public void testFullChain(TestContext context) throws IOException, InterruptedException {
        final BrokerFiller brokerFiller = new BrokerFiller(MainVerticleIT.vertx);
        final Async asyncSend = context.async();
        Future<?> brokerFillerFuture = brokerFiller.setUp();
        brokerFillerFuture.setHandler(ar -> {
            if (ar.succeeded()) {
                asyncSend.complete();
            } else {
                context.fail(brokerFillerFuture.cause());
            }
        });
        asyncSend.awaitSuccess(5000);
        Thread.sleep(5000);
        Async asyncHistoryCount = context.async();
        MainVerticleIT.mongoClient.count(AccountMarginModel.MONGO_HISTORY_COLLECTION, new JsonObject(), ar -> {
            if (ar.succeeded()) {
                context.assertEquals(1704L, ar.result());
                asyncHistoryCount.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncHistoryCount.awaitSuccess(5000);

    }

    @AfterClass
    public static void tearDown(TestContext context) {
        MainVerticleIT.vertx.close(context.asyncAssertSuccess());
    }
}
