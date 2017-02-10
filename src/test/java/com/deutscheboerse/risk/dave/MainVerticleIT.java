package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.model.AccountMarginModel;
import com.deutscheboerse.risk.dave.model.LiquiGroupMarginModel;
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
import java.util.concurrent.atomic.AtomicLong;

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
        MainVerticleIT.createMongoClient();
    }

    private static void createMongoClient() {
        JsonObject mongoConfig = new JsonObject()
                .put("db_name", MainVerticleIT.DB_NAME)
                .put("port", MainVerticleIT.DB_PORT)
                .put("waitQueueMultiple", 20000);
        MainVerticleIT.mongoClient = MongoClient.createShared(MainVerticleIT.vertx, mongoConfig);
    }

    private DeploymentOptions createDeploymentOptions() {
        JsonObject brokerConfig = new JsonObject()
                .put("port", BROKER_PORT)
                .put("listeners", new JsonObject()
                        .put("accountMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEAccountMargin")
                        .put("liquiGroupMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupMargin"));
        JsonObject mongoConfig = new JsonObject()
                .put("dbName", MainVerticleIT.DB_NAME)
                .put("port", MainVerticleIT.DB_PORT);
        JsonObject config = new JsonObject()
                .put("broker", brokerConfig)
                .put("mongo", mongoConfig);
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        return options;
    }

    @Test
    public void testFullChain(TestContext context) throws IOException, InterruptedException {
        DeploymentOptions options = createDeploymentOptions();
        MainVerticleIT.vertx.deployVerticle(MainVerticle.class.getName(), options, context.asyncAssertSuccess());
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
        asyncSend.awaitSuccess(30000);
        this.testCountInCollection(context, AccountMarginModel.MONGO_HISTORY_COLLECTION, 1704);
        this.testCountInCollection(context, AccountMarginModel.MONGO_LATEST_COLLECTION, 1704);
        this.testCountInCollection(context, LiquiGroupMarginModel.MONGO_HISTORY_COLLECTION, 2171);
        this.testCountInCollection(context, LiquiGroupMarginModel.MONGO_LATEST_COLLECTION, 2171);
    }

    @Test
    public void testFailedDeployment(TestContext context) {
        DeploymentOptions options = createDeploymentOptions();
        options.getConfig().getJsonObject("broker").put("hostname", "nonexisting");
        MainVerticleIT.vertx.deployVerticle(MainVerticle.class.getName(), options, context.asyncAssertFailure());
    }

    private void testCountInCollection(TestContext  context, String collection, long count) {
        AtomicLong currentCount = new AtomicLong();
        int tries = 0;
        while (currentCount.get() != count && tries < 10) {
            Async asyncHistoryCount = context.async();
            MainVerticleIT.mongoClient.count(collection, new JsonObject(), ar -> {
                if (ar.succeeded()) {
                    currentCount.set(ar.result());
                    if (currentCount.get() == count) {
                        asyncHistoryCount.complete();
                    }
                } else {
                    context.fail(ar.cause());
                }
            });
            try {
                asyncHistoryCount.await(1000);
            } catch (Exception ignored) {
                asyncHistoryCount.complete();
            }
            tries++;
        }
        context.assertEquals(count, currentCount.get());
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        MainVerticleIT.vertx.close(context.asyncAssertSuccess());
    }
}
