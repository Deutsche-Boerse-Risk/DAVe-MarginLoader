package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.model.*;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(VertxUnitRunner.class)
public class MainVerticleIT extends BaseTest {
    private Vertx vertx;

    @Before
    public void setUp() {
        this.vertx = Vertx.vertx();
    }

    private MongoClient createMongoClient(JsonObject mongoVerticleConfig) {
        return MongoClient.createShared(this.vertx, BaseTest.getMongoClientConfig(mongoVerticleConfig));
    }

    private DeploymentOptions createDeploymentOptions() {
        return new DeploymentOptions().setConfig(BaseTest.getGlobalConfig());
    }

    @Test
    public void testFullChain(TestContext context) throws IOException, InterruptedException {
        Async mainVerticleAsync = context.async();
        DeploymentOptions options = createDeploymentOptions();
        this.vertx.deployVerticle(MainVerticle.class.getName(), options, ar -> {
            if (ar.succeeded()) {
                mainVerticleAsync.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        mainVerticleAsync.awaitSuccess(30000);
        MongoClient mongoClient = this.createMongoClient(options.getConfig().getJsonObject("mongo"));
        final BrokerFiller brokerFiller = new BrokerFiller(this.vertx);
        brokerFiller.setUpAllQueues(context.asyncAssertSuccess());
        this.testCountInCollection(context, mongoClient, AccountMarginModel.MONGO_HISTORY_COLLECTION, 1704);
        this.testCountInCollection(context, mongoClient, AccountMarginModel.MONGO_LATEST_COLLECTION, 1704);
        this.testCountInCollection(context, mongoClient, LiquiGroupMarginModel.MONGO_HISTORY_COLLECTION, 2171);
        this.testCountInCollection(context, mongoClient, LiquiGroupMarginModel.MONGO_LATEST_COLLECTION, 2171);
        this.testCountInCollection(context, mongoClient, LiquiGroupSplitMarginModel.MONGO_HISTORY_COLLECTION, 2472);
        this.testCountInCollection(context, mongoClient, LiquiGroupSplitMarginModel.MONGO_LATEST_COLLECTION, 2472);
        this.testCountInCollection(context, mongoClient, PoolMarginModel.MONGO_HISTORY_COLLECTION, 270);
        this.testCountInCollection(context, mongoClient, PoolMarginModel.MONGO_LATEST_COLLECTION, 270);
        this.testCountInCollection(context, mongoClient, PositionReportModel.MONGO_HISTORY_COLLECTION, 3596);
        this.testCountInCollection(context, mongoClient, PositionReportModel.MONGO_LATEST_COLLECTION, 3596);
        this.testCountInCollection(context, mongoClient, RiskLimitUtilizationModel.MONGO_HISTORY_COLLECTION, 6);
        this.testCountInCollection(context, mongoClient, RiskLimitUtilizationModel.MONGO_LATEST_COLLECTION, 6);
    }

    @Test
    public void testFailedDeployment(TestContext context) {
        DeploymentOptions options = createDeploymentOptions();
        options.getConfig().getJsonObject("broker").put("hostname", "nonexisting");
        this.vertx.deployVerticle(MainVerticle.class.getName(), options, context.asyncAssertFailure());
    }

    private void testCountInCollection(TestContext  context, MongoClient mongoClient, String collection, long count) {
        AtomicLong currentCount = new AtomicLong();
        int tries = 0;
        while (currentCount.get() != count && tries < 60) {
            Async asyncHistoryCount = context.async();
            mongoClient.count(collection, new JsonObject(), ar -> {
                if (ar.succeeded()) {
                    currentCount.set(ar.result());
                    if (currentCount.get() == count && !asyncHistoryCount.isCompleted()) {
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
