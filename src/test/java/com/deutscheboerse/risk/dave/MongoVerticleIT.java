package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.model.AccountMarginModel;
import com.deutscheboerse.risk.dave.model.LiquiGroupMarginModel;
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
    private static final int DB_PORT =  Integer.getInteger("mongodb.port", 27017);
    private static Vertx vertx;
    private static MongoClient mongoClient;

    @BeforeClass
    public static void setUp(TestContext context) {
        MongoVerticleIT.vertx = Vertx.vertx();
        JsonObject config = new JsonObject()
                .put("dbName", "DAVe-Test" + UUID.randomUUID().getLeastSignificantBits())
                .put("port", MongoVerticleIT.DB_PORT);
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        MongoVerticleIT.vertx.deployVerticle(MongoVerticle.class.getName(), options, context.asyncAssertSuccess());

        JsonObject mongoConfig = new JsonObject();
        mongoConfig.put("db_name", config.getString("dbName"));
        mongoConfig.put("useObjectId", true);
        mongoConfig.put("port", config.getInteger("port"));
        mongoConfig.put("waitQueueMultiple", 20000);
        MongoVerticleIT.mongoClient = MongoClient.createShared(MongoVerticleIT.vertx, mongoConfig);
    }

    @Test
    public void checkCollectionsExist(TestContext context) {
        List<String> requiredCollections = new ArrayList<>();
        requiredCollections.add(AccountMarginModel.MONGO_HISTORY_COLLECTION);
        requiredCollections.add(AccountMarginModel.MONGO_LATEST_COLLECTION);
        requiredCollections.add(LiquiGroupMarginModel.MONGO_HISTORY_COLLECTION);
        requiredCollections.add(LiquiGroupMarginModel.MONGO_LATEST_COLLECTION);
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
        asyncStore.awaitSuccess(30000);
        this.checkCountInCollection(context, AccountMarginModel.MONGO_HISTORY_COLLECTION, 1704);
        this.checkCountInCollection(context, AccountMarginModel.MONGO_LATEST_COLLECTION, 1704);
        this.checkAccountMarginHistoryCollectionQuery(context);
        this.checkAccountMarginLatestCollectionQuery(context);
    }

    @Test
    public void testLiquiGroupMarginStore(TestContext context) throws IOException {
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        PrismaReports.registerAllExtensions(registry);
        String dataFilePath = String.format("%s/%03d.bin", MongoVerticleIT.class.getResource("liquiGroupMargin").getPath(), 1);
        byte[] gpbBytes = Files.readAllBytes(Paths.get(dataFilePath));
        ObjectList.GPBObjectList gpbObjectList = ObjectList.GPBObjectList.parseFrom(gpbBytes, registry);
        PrismaReports.PrismaHeader header = gpbObjectList.getHeader().getExtension(PrismaReports.prismaHeader);
        Async asyncStore = context.async(2171);
        gpbObjectList.getItemList().forEach(gpbObject -> {
            PrismaReports.LiquiGroupMargin liquiGroupMarginData = gpbObject.getExtension(PrismaReports.liquiGroupMargin);
            LiquiGroupMarginModel liquiGroupMarginModel = new LiquiGroupMarginModel(header, liquiGroupMarginData);
            vertx.eventBus().send(LiquiGroupMarginModel.EB_STORE_ADDRESS, liquiGroupMarginModel, ar ->  {
                if (ar.succeeded()) {
                    asyncStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncStore.awaitSuccess(30000);
        this.checkCountInCollection(context, LiquiGroupMarginModel.MONGO_HISTORY_COLLECTION, 2171);
        this.checkCountInCollection(context, LiquiGroupMarginModel.MONGO_LATEST_COLLECTION, 2171);
        this.checkLiquiGroupMarginHistoryCollectionQuery(context);
        this.checkLiquiGroupMarginLatestCollectionQuery(context);
    }

    private void checkCountInCollection(TestContext context, String collection, long count) {
        Async asyncHistoryCount = context.async();
        MongoVerticleIT.mongoClient.count(collection, new JsonObject(), ar -> {
            if (ar.succeeded()) {
                context.assertEquals(count, ar.result());
                asyncHistoryCount.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncHistoryCount.awaitSuccess(5000);
    }

    private void checkAccountMarginHistoryCollectionQuery(TestContext context) {
        JsonObject param = new JsonObject();
        param.put("clearer", "BERFR");
        param.put("member", "BERFR");
        param.put("account", "A5");
        param.put("marginCurrency", "EUR");

        Async asyncQuery = context.async();
        MongoVerticleIT.mongoClient.find(AccountMarginModel.MONGO_HISTORY_COLLECTION, param, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(1, ar.result().size());
                JsonObject result = ar.result().get(0);

                context.assertEquals(5, result.getInteger("snapshotID"));
                context.assertEquals(20091215, result.getInteger("businessDate"));
                context.assertEquals(new JsonObject().put("$date", "2017-02-07T11:08:41.933Z"), result.getJsonObject("timestamp"));
                context.assertEquals("BERFR", result.getString("clearer"));
                context.assertEquals("BERFR", result.getString("member"));
                context.assertEquals("A5", result.getString("account"));
                context.assertEquals("EUR", result.getString("marginCurrency"));
                context.assertEquals("EUR", result.getString("clearingCurrency"));
                context.assertEquals("default", result.getString("pool"));
                context.assertEquals(6.378857805534203E8, result.getDouble("marginReqInMarginCurr"));
                context.assertEquals(6.378857805534203E8, result.getDouble("marginReqInCrlCurr"));
                context.assertEquals(6.378857805534203E8, result.getDouble("unadjustedMarginRequirement"));
                context.assertEquals(0.0, result.getDouble("variationPremiumPayment"));
                asyncQuery.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncQuery.awaitSuccess(5000);
    }

    private void checkAccountMarginLatestCollectionQuery(TestContext context) {
        JsonObject param = new JsonObject();
        param.put("clearer", "BERFR");
        param.put("member", "BERFR");
        param.put("account", "A5");
        param.put("marginCurrency", "EUR");

        Async asyncQuery = context.async();
        MongoVerticleIT.mongoClient.find(AccountMarginModel.MONGO_LATEST_COLLECTION, param, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(1, ar.result().size());
                JsonObject result = ar.result().get(0);

                context.assertEquals(5, result.getInteger("snapshotID"));
                context.assertEquals(20091215, result.getInteger("businessDate"));
                context.assertEquals(new JsonObject().put("$date", "2017-02-07T11:08:41.933Z"), result.getJsonObject("timestamp"));
                context.assertEquals("BERFR", result.getString("clearer"));
                context.assertEquals("BERFR", result.getString("member"));
                context.assertEquals("A5", result.getString("account"));
                context.assertEquals("EUR", result.getString("marginCurrency"));
                context.assertEquals("EUR", result.getString("clearingCurrency"));
                context.assertEquals("default", result.getString("pool"));
                context.assertEquals(6.378857805534203E8, result.getDouble("marginReqInMarginCurr"));
                context.assertEquals(6.378857805534203E8, result.getDouble("marginReqInCrlCurr"));
                context.assertEquals(6.378857805534203E8, result.getDouble("unadjustedMarginRequirement"));
                context.assertEquals(0.0, result.getDouble("variationPremiumPayment"));
                asyncQuery.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncQuery.awaitSuccess(5000);
    }

    private void checkLiquiGroupMarginHistoryCollectionQuery(TestContext context) {
        JsonObject param = new JsonObject();
        param.put("clearer", "ABCFR");
        param.put("member", "ABCFR");
        param.put("account", "PP");
        param.put("marginClass", "ECC01");
        param.put("marginCurrency", "EUR");

        Async asyncQuery = context.async();
        MongoVerticleIT.mongoClient.find(LiquiGroupMarginModel.MONGO_HISTORY_COLLECTION, param, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(1, ar.result().size());
                JsonObject result = ar.result().get(0);

                context.assertEquals(5, result.getInteger("snapshotID"));
                context.assertEquals(20091215, result.getInteger("businessDate"));
                context.assertEquals(new JsonObject().put("$date", "2017-02-07T11:08:41.933Z"), result.getJsonObject("timestamp"));
                context.assertEquals("ABCFR", result.getString("clearer"));
                context.assertEquals("ABCFR", result.getString("member"));
                context.assertEquals("PP", result.getString("account"));
                context.assertEquals("ECC01", result.getString("marginClass"));
                context.assertEquals("EUR", result.getString("marginCurrency"));
                context.assertEquals("", result.getString("marginGroup"));
                context.assertEquals(135000.5, result.getDouble("premiumMargin"));
                context.assertEquals(0.0, result.getDouble("currentLiquidatingMargin"));
                context.assertEquals(0.0, result.getDouble("futuresSpreadMargin"));
                context.assertEquals(14914.841270178167, result.getDouble("additionalMargin"));
                context.assertEquals(149915.34127017818, result.getDouble("unadjustedMarginRequirement"));
                context.assertEquals(0.0, result.getDouble("variationPremiumPayment"));
                asyncQuery.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncQuery.awaitSuccess(5000);
    }

    private void checkLiquiGroupMarginLatestCollectionQuery(TestContext context) {
        JsonObject param = new JsonObject();
        param.put("clearer", "ABCFR");
        param.put("member", "ABCFR");
        param.put("account", "PP");
        param.put("marginClass", "ECC01");
        param.put("marginCurrency", "EUR");

        Async asyncQuery = context.async();
        MongoVerticleIT.mongoClient.find(LiquiGroupMarginModel.MONGO_LATEST_COLLECTION, param, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(1, ar.result().size());
                JsonObject result = ar.result().get(0);

                context.assertEquals(5, result.getInteger("snapshotID"));
                context.assertEquals(20091215, result.getInteger("businessDate"));
                context.assertEquals(new JsonObject().put("$date", "2017-02-07T11:08:41.933Z"), result.getJsonObject("timestamp"));
                context.assertEquals("ABCFR", result.getString("clearer"));
                context.assertEquals("ABCFR", result.getString("member"));
                context.assertEquals("PP", result.getString("account"));
                context.assertEquals("ECC01", result.getString("marginClass"));
                context.assertEquals("EUR", result.getString("marginCurrency"));
                context.assertEquals("", result.getString("marginGroup"));
                context.assertEquals(135000.5, result.getDouble("premiumMargin"));
                context.assertEquals(0.0, result.getDouble("currentLiquidatingMargin"));
                context.assertEquals(0.0, result.getDouble("futuresSpreadMargin"));
                context.assertEquals(14914.841270178167, result.getDouble("additionalMargin"));
                context.assertEquals(149915.34127017818, result.getDouble("unadjustedMarginRequirement"));
                context.assertEquals(0.0, result.getDouble("variationPremiumPayment"));
                asyncQuery.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncQuery.awaitSuccess(5000);
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        MongoVerticleIT.vertx.close(context.asyncAssertSuccess());
    }
}
