package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.model.AccountMarginModel;
import com.google.protobuf.ExtensionRegistry;
import io.vertx.core.DeploymentOptions;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class MongoVerticleIT {
    private static final String DB_PORT =  System.getProperty("mongodb.port", "27017");
    private static Vertx vertx;
    private static MongoClient mongoClient;

    @BeforeClass
    public static void setUp(TestContext context) {
        MongoVerticleIT.vertx = Vertx.vertx();
        JsonObject config = new JsonObject()
                .put("dbName", "DAVe-Test" + UUID.randomUUID().getLeastSignificantBits())
                .put("connectionUrl", "mongodb://localhost:" + MongoVerticleIT.DB_PORT);
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        MongoVerticleIT.vertx.deployVerticle(MongoVerticle.class.getName(), options, context.asyncAssertSuccess());

        JsonObject mongoConfig = new JsonObject();
        mongoConfig.put("db_name", config.getString("dbName"));
        mongoConfig.put("useObjectId", true);
        mongoConfig.put("connection_string", config.getString("connectionUrl"));
        MongoVerticleIT.mongoClient = MongoClient.createShared(MongoVerticleIT.vertx, mongoConfig);
    }

    @Test
    public void checkCollectionsExist(TestContext context) {
        List<String> requiredCollections = new ArrayList<>();
        requiredCollections.add("AccountMargin");
        requiredCollections.add("AccountMargin.latest");
        final Async async = context.async();
        MongoVerticleIT.mongoClient.getCollections(ar -> {
            if (ar.succeeded()) {
                if (ar.result().containsAll(requiredCollections)) {
                    async.complete();
                } else {
                    requiredCollections.removeAll(ar.result());
                    context.fail("Following collections were not created: " + requiredCollections);
                }
            } else {
                context.fail(ar.cause());
            }
        });
    }

    @Test
    public void testAccountMarginStore(TestContext context) throws IOException {
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        PrismaReports.registerAllExtensions(registry);
        String dataFilePath = String.format("%s/%03d.bin", MongoVerticleIT.class.getResource("accountMargin").getPath(), 1);
        byte[] gpbBytes = Files.readAllBytes(Paths.get(dataFilePath));
        ObjectList.GPBObjectList gpbObjectList = ObjectList.GPBObjectList.parseFrom(gpbBytes, registry);
        PrismaReports.PrismaHeader header = gpbObjectList.getHeader().getExtension(PrismaReports.prismaHeader);
        Async asyncStore = context.async(1704);
        gpbObjectList.getItemList().forEach(gpbObject -> {
            PrismaReports.AccountMargin accountMarginData = gpbObject.getExtension(PrismaReports.accountMargin);
            AccountMarginModel accountMarginModel = new AccountMarginModel(header, accountMarginData);
            vertx.eventBus().send(AccountMarginModel.EB_STORE_ADDRESS, accountMarginModel, ar ->  {
                if (ar.succeeded()) {
                    asyncStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncStore.awaitSuccess(5000);
        Async asyncHistoryCount = context.async();
        MongoVerticleIT.mongoClient.count(AccountMarginModel.MONGO_HISTORY_COLLECTION, new JsonObject(), ar -> {
            if (ar.succeeded()) {
                context.assertEquals(1704L, ar.result());
                asyncHistoryCount.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncHistoryCount.awaitSuccess(5000);
        Async asyncLatestCount = context.async();
        MongoVerticleIT.mongoClient.count(AccountMarginModel.MONGO_LATEST_COLLECTION, new JsonObject(), ar -> {
            if (ar.succeeded()) {
                context.assertEquals(1704L, ar.result());
                asyncLatestCount.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncLatestCount.awaitSuccess(5000);
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        MongoVerticleIT.vertx.close(context.asyncAssertSuccess());
    }
}
