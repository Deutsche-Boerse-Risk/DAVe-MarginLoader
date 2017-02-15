package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.model.AccountMarginModel;
import com.deutscheboerse.risk.dave.model.LiquiGroupMarginModel;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(VertxUnitRunner.class)
public class MainVerticleIT {
    private static final int BROKER_PORT = Integer.getInteger("cil.tcpport", 5672);
    private static final String DB_NAME = "DAVe-Test" + UUID.randomUUID().getLeastSignificantBits();
    private static final int DB_PORT =  Integer.getInteger("mongodb.port", 27017);
    private Vertx vertx;
    private MongoClient mongoClient;

    @Before
    public void setUp() {
        this.vertx = Vertx.vertx();
        this.createMongoClient();
    }

    private void createMongoClient() {
        JsonObject mongoConfig = new JsonObject()
                .put("db_name", MainVerticleIT.DB_NAME)
                .put("connection_string", String.format("mongodb://localhost:%s/?waitqueuemultiple=%d", MainVerticleIT.DB_PORT, 20000));
        this.mongoClient = MongoClient.createShared(this.vertx, mongoConfig);
    }

    private DeploymentOptions createDeploymentOptions() {
        JsonObject brokerConfig = new JsonObject()
                .put("port", BROKER_PORT)
                .put("listeners", new JsonObject()
                        .put("accountMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEAccountMargin")
                        .put("liquiGroupMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupMargin"));
        JsonObject mongoConfig = new JsonObject()
                .put("dbName", MainVerticleIT.DB_NAME)
                .put("connectionUrl", String.format("mongodb://localhost:%s/?waitqueuemultiple=%d", MainVerticleIT.DB_PORT, 20000));
        JsonObject config = new JsonObject()
                .put("broker", brokerConfig)
                .put("mongo", mongoConfig);
        return new DeploymentOptions().setConfig(config);
    }

    @Test
    public void testFullChain(TestContext context) throws IOException, InterruptedException {
        DeploymentOptions options = createDeploymentOptions();
        this.vertx.deployVerticle(MainVerticle.class.getName(), options, context.asyncAssertSuccess());
        final BrokerFiller brokerFiller = new BrokerFiller(this.vertx);
        brokerFiller.setUpAllQueues(context.asyncAssertSuccess());
        this.testCountInCollection(context, AccountMarginModel.MONGO_HISTORY_COLLECTION, 1704);
        this.testCountInCollection(context, AccountMarginModel.MONGO_LATEST_COLLECTION, 1704);
        this.testCountInCollection(context, LiquiGroupMarginModel.MONGO_HISTORY_COLLECTION, 2171);
        this.testCountInCollection(context, LiquiGroupMarginModel.MONGO_LATEST_COLLECTION, 2171);
    }

    @Test
    public void testFailedDeployment(TestContext context) {
        DeploymentOptions options = createDeploymentOptions();
        options.getConfig().getJsonObject("broker").put("hostname", "nonexisting");
        this.vertx.deployVerticle(MainVerticle.class.getName(), options, context.asyncAssertFailure());
    }

    private void testCountInCollection(TestContext  context, String collection, long count) {
        AtomicLong currentCount = new AtomicLong();
        int tries = 0;
        while (currentCount.get() != count && tries < 60) {
            Async asyncHistoryCount = context.async();
            this.mongoClient.count(collection, new JsonObject(), ar -> {
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

    @After
    public void cleanup(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }
}
