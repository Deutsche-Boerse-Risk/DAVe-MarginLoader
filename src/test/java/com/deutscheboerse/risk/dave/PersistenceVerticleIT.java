package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.model.AccountMarginModel;
import com.deutscheboerse.risk.dave.model.LiquiGroupMarginModel;
import com.deutscheboerse.risk.dave.model.ModelType;
import com.deutscheboerse.risk.dave.model.PoolMarginModel;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import com.google.protobuf.ExtensionRegistry;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
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
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

@RunWith(VertxUnitRunner.class)
public class PersistenceVerticleIT {
    private static final int DB_PORT = Integer.getInteger("mongodb.port", 27017);
    private static Vertx vertx;
    private static MongoClient mongoClient;
    private static PersistenceService persistenceService;

    @BeforeClass
    public static void setUp(TestContext context) {
        PersistenceVerticleIT.vertx = Vertx.vertx();
        JsonObject config = new JsonObject()
                .put("dbName", "DAVe-Test" + UUID.randomUUID().getLeastSignificantBits())
                .put("connectionUrl", String.format("mongodb://localhost:%s/?waitqueuemultiple=%d", PersistenceVerticleIT.DB_PORT, 20000));
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        PersistenceVerticleIT.vertx.deployVerticle(PersistenceVerticle.class.getName(), options, context.asyncAssertSuccess());

        JsonObject mongoConfig = new JsonObject();
        mongoConfig.put("db_name", config.getString("dbName"));
        mongoConfig.put("useObjectId", true);
        mongoConfig.put("connection_string", config.getString("connectionUrl"));
        PersistenceVerticleIT.mongoClient = MongoClient.createShared(PersistenceVerticleIT.vertx, mongoConfig);
        PersistenceVerticleIT.persistenceService = PersistenceService.createProxy(vertx, "persistenceService");

    }

    @Test
    public void checkCollectionsExist(TestContext context) {
        List<String> requiredCollections = new ArrayList<>();
        requiredCollections.add(AccountMarginModel.MONGO_HISTORY_COLLECTION);
        requiredCollections.add(AccountMarginModel.MONGO_LATEST_COLLECTION);
        requiredCollections.add(LiquiGroupMarginModel.MONGO_HISTORY_COLLECTION);
        requiredCollections.add(LiquiGroupMarginModel.MONGO_LATEST_COLLECTION);
        requiredCollections.add(PoolMarginModel.MONGO_HISTORY_COLLECTION);
        requiredCollections.add(PoolMarginModel.MONGO_LATEST_COLLECTION);
        final Async async = context.async();
        PersistenceVerticleIT.mongoClient.getCollections(ar -> {
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
        String dataFilePath = String.format("%s/%03d.bin", PersistenceVerticleIT.class.getResource("accountMargin").getPath(), 1);
        byte[] gpbBytes = Files.readAllBytes(Paths.get(dataFilePath));
        ObjectList.GPBObjectList gpbObjectList = ObjectList.GPBObjectList.parseFrom(gpbBytes, registry);
        PrismaReports.PrismaHeader header = gpbObjectList.getHeader().getExtension(PrismaReports.prismaHeader);
        Async asyncStore = context.async(1704);
        gpbObjectList.getItemList().forEach(gpbObject -> {
            PrismaReports.AccountMargin accountMarginData = gpbObject.getExtension(PrismaReports.accountMargin);
            AccountMarginModel accountMarginModel = new AccountMarginModel(header, accountMarginData);
            persistenceService.store(accountMarginModel, ModelType.ACCOUNT_MARGIN_MODEL, ar -> {
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
        String dataFilePath = String.format("%s/%03d.bin", PersistenceVerticleIT.class.getResource("liquiGroupMargin").getPath(), 1);
        byte[] gpbBytes = Files.readAllBytes(Paths.get(dataFilePath));
        ObjectList.GPBObjectList gpbObjectList = ObjectList.GPBObjectList.parseFrom(gpbBytes, registry);
        PrismaReports.PrismaHeader header = gpbObjectList.getHeader().getExtension(PrismaReports.prismaHeader);
        Async asyncStore = context.async(2171);
        gpbObjectList.getItemList().forEach(gpbObject -> {
            PrismaReports.LiquiGroupMargin liquiGroupMarginData = gpbObject.getExtension(PrismaReports.liquiGroupMargin);
            LiquiGroupMarginModel liquiGroupMarginModel = new LiquiGroupMarginModel(header, liquiGroupMarginData);
            persistenceService.store(liquiGroupMarginModel, ModelType.LIQUI_GROUP_MARGIN_MODEL, ar -> {
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

    @Test
    public void testPoolMarginStore(TestContext context) throws IOException {
        Async asyncFirstSnapshotStore = context.async(270);
        readTTSaveFiles("poolMargin", IntStream.rangeClosed(1, 1), (header, gpbObject) -> {
            PrismaReports.PoolMargin poolMarginData = gpbObject.getExtension(PrismaReports.poolMargin);
            PoolMarginModel poolMarginModel = new PoolMarginModel(header, poolMarginData);
            persistenceService.store(poolMarginModel, ModelType.POOL_MARGIN_MODEL, ar -> {
                if (ar.succeeded()) {
                    asyncFirstSnapshotStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncFirstSnapshotStore.awaitSuccess(30000);
        Async asyncSecondSnapshotStore = context.async(270);
        readTTSaveFiles("poolMargin", IntStream.rangeClosed(2, 2), (header, gpbObject) -> {
            PrismaReports.PoolMargin poolMarginData = gpbObject.getExtension(PrismaReports.poolMargin);
            PoolMarginModel poolMarginModel = new PoolMarginModel(header, poolMarginData);
            persistenceService.store(poolMarginModel, ModelType.POOL_MARGIN_MODEL, ar -> {
                if (ar.succeeded()) {
                    asyncSecondSnapshotStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncSecondSnapshotStore.awaitSuccess(30000);
        this.checkCountInCollection(context, PoolMarginModel.MONGO_HISTORY_COLLECTION, 540);
        this.checkCountInCollection(context, PoolMarginModel.MONGO_LATEST_COLLECTION, 270);
        this.checkPoolMarginHistoryCollectionQuery(context);
        this.checkPoolMarginLatestCollectionQuery(context);
    }

    /**
     * @param folderName "accountMargin", "liquiGroupMargin" or "poolMargin"
     * @param range 1 for the first snapshot, etc.
     */
    private void readTTSaveFiles(String folderName, IntStream range, BiConsumer<? super PrismaReports.PrismaHeader, ? super ObjectList.GPBObject> consumer) {
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        PrismaReports.registerAllExtensions(registry);
        range
             .mapToObj(i -> String.format("%s/%03d.bin", PersistenceVerticleIT.class.getResource(folderName).getPath(), i))
             .forEach(path -> {
                 try {
                     byte[] gpbBytes = Files.readAllBytes(Paths.get(path));
                     ObjectList.GPBObjectList gpbObjectList = ObjectList.GPBObjectList.parseFrom(gpbBytes, registry);
                     PrismaReports.PrismaHeader header = gpbObjectList.getHeader().getExtension(PrismaReports.prismaHeader);
                     gpbObjectList.getItemList().forEach(gpbObject -> consumer.accept(header, gpbObject));
                 } catch (IOException e) {
                     e.printStackTrace();
                 }
             });
    }

    private void checkCountInCollection(TestContext context, String collection, long count) {
        Async asyncHistoryCount = context.async();
        PersistenceVerticleIT.mongoClient.count(collection, new JsonObject(), ar -> {
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
        PersistenceVerticleIT.mongoClient.find(AccountMarginModel.MONGO_HISTORY_COLLECTION, param, ar -> {
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
        PersistenceVerticleIT.mongoClient.find(AccountMarginModel.MONGO_LATEST_COLLECTION, param, ar -> {
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
        PersistenceVerticleIT.mongoClient.find(LiquiGroupMarginModel.MONGO_HISTORY_COLLECTION, param, ar -> {
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
        PersistenceVerticleIT.mongoClient.find(LiquiGroupMarginModel.MONGO_LATEST_COLLECTION, param, ar -> {
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

    private void checkPoolMarginHistoryCollectionQuery(TestContext context) {
        JsonObject param = new JsonObject()
                .put("clearer", "USWFC")
                .put("pool", "default")
                .put("marginCurrency", "CHF");
        FindOptions findOptions = new FindOptions()
                .setSort(new JsonObject().put("snapshotID", 1));

        Async asyncQuery = context.async();
        PersistenceVerticleIT.mongoClient.findWithOptions(PoolMarginModel.MONGO_HISTORY_COLLECTION, param, findOptions, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(2, ar.result().size());
                JsonObject result1 = ar.result().get(0);

                context.assertEquals(10, result1.getInteger("snapshotID"));
                context.assertEquals(20091215, result1.getInteger("businessDate"));
                context.assertEquals(new JsonObject().put("$date", "2017-02-15T15:27:02.43Z"), result1.getJsonObject("timestamp"));
                context.assertEquals("USWFC", result1.getString("clearer"));
                context.assertEquals("default", result1.getString("pool"));
                context.assertEquals("CHF", result1.getString("marginCurrency"));
                context.assertEquals(0.0, result1.getDouble("requiredMargin"));
                context.assertEquals(116938762.025, result1.getDouble("cashCollateralAmount"));
                context.assertEquals(0.0, result1.getDouble("adjustedSecurities"));
                context.assertEquals(0.0, result1.getDouble("adjustedGuarantee"));
                context.assertEquals(116938762.025, result1.getDouble("overUnderInMarginCurr"));
                context.assertEquals(87509625.13167715, result1.getDouble("overUnderInClrRptCurr"));
                context.assertEquals(116938762.025, result1.getDouble("variPremInMarginCurr"));
                context.assertEquals(0.748337194753, result1.getDouble("adjustedExchangeRate"));
                context.assertEquals("USWFC", result1.getString("poolOwner"));
                asyncQuery.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncQuery.awaitSuccess(5000);
    }

    private void checkPoolMarginLatestCollectionQuery(TestContext context) {
        JsonObject param = new JsonObject()
            .put("clearer", "USWFC")
            .put("pool", "default")
            .put("marginCurrency", "CHF");

        Async asyncQuery = context.async();
        PersistenceVerticleIT.mongoClient.find(PoolMarginModel.MONGO_LATEST_COLLECTION, param, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(1, ar.result().size());
                JsonObject result1 = ar.result().get(0);

                context.assertEquals(11, result1.getInteger("snapshotID"));
                context.assertEquals(20091215, result1.getInteger("businessDate"));
                context.assertEquals(new JsonObject().put("$date", "2017-02-15T15:28:50.03Z"), result1.getJsonObject("timestamp"));
                context.assertEquals("USWFC", result1.getString("clearer"));
                context.assertEquals("default", result1.getString("pool"));
                context.assertEquals("CHF", result1.getString("marginCurrency"));
                context.assertEquals(0.0, result1.getDouble("requiredMargin"));
                context.assertEquals(116938762.025, result1.getDouble("cashCollateralAmount"));
                context.assertEquals(0.0, result1.getDouble("adjustedSecurities"));
                context.assertEquals(0.0, result1.getDouble("adjustedGuarantee"));
                context.assertEquals(116938762.025, result1.getDouble("overUnderInMarginCurr"));
                context.assertEquals(87509625.13167715, result1.getDouble("overUnderInClrRptCurr"));
                context.assertEquals(116938762.025, result1.getDouble("variPremInMarginCurr"));
                context.assertEquals(0.748337194753, result1.getDouble("adjustedExchangeRate"));
                context.assertEquals("USWFC", result1.getString("poolOwner"));
                asyncQuery.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncQuery.awaitSuccess(5000);
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        PersistenceVerticleIT.vertx.close(context.asyncAssertSuccess());
    }
}
