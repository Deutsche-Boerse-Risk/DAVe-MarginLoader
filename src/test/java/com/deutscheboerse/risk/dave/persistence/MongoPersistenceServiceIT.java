package com.deutscheboerse.risk.dave.persistence;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.BaseTest;
import com.deutscheboerse.risk.dave.MainVerticleIT;
import com.deutscheboerse.risk.dave.model.*;
import com.google.protobuf.ExtensionRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.serviceproxy.ProxyHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

@RunWith(VertxUnitRunner.class)
public class MongoPersistenceServiceIT extends BaseTest {
    private static Vertx vertx;
    private static MongoClient mongoClient;
    private static PersistenceService persistenceProxy;
    private static final double DOUBLE_DELTA = 1e-12;
    private static MessageConsumer<JsonObject> persistenceServiceConsumer;

    @BeforeClass
    public static void setUp(TestContext context) {
        MongoPersistenceServiceIT.vertx = Vertx.vertx();
        JsonObject config = BaseTest.getMongoConfig();
        JsonObject mongoConfig = BaseTest.getMongoClientConfig(config);

        MongoPersistenceServiceIT.mongoClient = MongoClient.createShared(MongoPersistenceServiceIT.vertx, mongoConfig);

        PersistenceService mongoPersistenceService = new MongoPersistenceService(vertx);

        MongoPersistenceServiceIT.persistenceServiceConsumer = ProxyHelper.registerService(PersistenceService.class, vertx, mongoPersistenceService, PersistenceService.SERVICE_ADDRESS);
        MongoPersistenceServiceIT.persistenceProxy = ProxyHelper.createProxy(PersistenceService.class, vertx, PersistenceService.SERVICE_ADDRESS);

        mongoPersistenceService.initialize(config, context.asyncAssertSuccess());
    }

    @Test
    public void checkCollectionsExist(TestContext context) {
        List<String> requiredCollections = new ArrayList<>();
        requiredCollections.add(MongoPersistenceService.ACCOUNT_MARGIN_HISTORY_COLLECTION);
        requiredCollections.add(MongoPersistenceService.ACCOUNT_MARGIN_LATEST_COLLECTION);
        requiredCollections.add(MongoPersistenceService.LIQUI_GROUP_MARGIN_HISTORY_COLLECTION);
        requiredCollections.add(MongoPersistenceService.LIQUI_GROUP_MARGIN_LATEST_COLLECTION);
        requiredCollections.add(MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_HISTORY_COLLECTION);
        requiredCollections.add(MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_LATEST_COLLECTION);
        requiredCollections.add(MongoPersistenceService.POOL_MARGIN_HISTORY_COLLECTION);
        requiredCollections.add(MongoPersistenceService.POOL_MARGIN_LATEST_COLLECTION);
        requiredCollections.add(MongoPersistenceService.POSITION_REPORT_HISTORY_COLLECTION);
        requiredCollections.add(MongoPersistenceService.POSITION_REPORT_LATEST_COLLECTION);
        requiredCollections.add(MongoPersistenceService.RISK_LIMIT_UTILIZATION_HISTORY_COLLECTION);
        requiredCollections.add(MongoPersistenceService.RISK_LIMIT_UTILIZATION_LATEST_COLLECTION);
        final Async async = context.async();
        MongoPersistenceServiceIT.mongoClient.getCollections(ar -> {
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
    public void checkIndexesExist(TestContext context) {
        // one index for history and one for latest collection in each model
        final Async async = context.async(6 * 2);
        BiConsumer<String, JsonObject> indexCheck = (collectionName, expectedIndex) -> {
            MongoPersistenceServiceIT.mongoClient.listIndexes(collectionName, ar -> {
                if (ar.succeeded()) {
                    JsonArray result = ar.result();
                    Optional<Object> latestUniqueIndex = result.stream()
                            .filter(index -> index instanceof JsonObject)
                            .filter(index -> ((JsonObject) index).getString("name", "").equals("unique_idx"))
                            .filter(index -> ((JsonObject) index).getJsonObject("key", new JsonObject()).equals(expectedIndex))
                            .findFirst();
                    if (latestUniqueIndex.isPresent()) {
                        async.countDown();
                    } else {
                        context.fail("Missing unique index for collection " + collectionName);
                    }
                } else {
                    context.fail("Unable to list indexes from collection " + collectionName);
                }
            });
        };
        indexCheck.accept(MongoPersistenceService.ACCOUNT_MARGIN_HISTORY_COLLECTION, MongoPersistenceService.getHistoryUniqueIndex(new AccountMarginModel()));
        indexCheck.accept(MongoPersistenceService.ACCOUNT_MARGIN_LATEST_COLLECTION, MongoPersistenceService.getLatestUniqueIndex(new AccountMarginModel()));
        indexCheck.accept(MongoPersistenceService.LIQUI_GROUP_MARGIN_HISTORY_COLLECTION, MongoPersistenceService.getHistoryUniqueIndex(new LiquiGroupMarginModel()));
        indexCheck.accept(MongoPersistenceService.LIQUI_GROUP_MARGIN_LATEST_COLLECTION, MongoPersistenceService.getLatestUniqueIndex(new LiquiGroupMarginModel()));
        indexCheck.accept(MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_HISTORY_COLLECTION, MongoPersistenceService.getHistoryUniqueIndex(new LiquiGroupSplitMarginModel()));
        indexCheck.accept(MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_LATEST_COLLECTION, MongoPersistenceService.getLatestUniqueIndex(new LiquiGroupSplitMarginModel()));
        indexCheck.accept(MongoPersistenceService.POOL_MARGIN_HISTORY_COLLECTION, MongoPersistenceService.getHistoryUniqueIndex(new PoolMarginModel()));
        indexCheck.accept(MongoPersistenceService.POOL_MARGIN_LATEST_COLLECTION, MongoPersistenceService.getLatestUniqueIndex(new PoolMarginModel()));
        indexCheck.accept(MongoPersistenceService.POSITION_REPORT_HISTORY_COLLECTION, MongoPersistenceService.getHistoryUniqueIndex(new PositionReportModel()));
        indexCheck.accept(MongoPersistenceService.POSITION_REPORT_LATEST_COLLECTION, MongoPersistenceService.getLatestUniqueIndex(new PositionReportModel()));
        indexCheck.accept(MongoPersistenceService.RISK_LIMIT_UTILIZATION_HISTORY_COLLECTION, MongoPersistenceService.getHistoryUniqueIndex(new RiskLimitUtilizationModel()));
        indexCheck.accept(MongoPersistenceService.RISK_LIMIT_UTILIZATION_LATEST_COLLECTION, MongoPersistenceService.getLatestUniqueIndex(new RiskLimitUtilizationModel()));
    }

    @Test
    public void testAccountMarginStore(TestContext context) throws IOException {
        Async asyncStore1 = context.async(1704);
        readTTSaveFile("accountMargin", 1, (header, gpbObject) -> {
            PrismaReports.AccountMargin accountMarginData = gpbObject.getExtension(PrismaReports.accountMargin);
            AccountMarginModel accountMarginModel = new AccountMarginModel(header, accountMarginData);
            persistenceProxy.storeAccountMargin(accountMarginModel, ar -> {
                if (ar.succeeded()) {
                    asyncStore1.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncStore1.awaitSuccess(30000);

        Async asyncStore2 = context.async(1704);
        readTTSaveFile("accountMargin", 2, (header, gpbObject) -> {
            PrismaReports.AccountMargin accountMarginData = gpbObject.getExtension(PrismaReports.accountMargin);
            AccountMarginModel accountMarginModel = new AccountMarginModel(header, accountMarginData);
            persistenceProxy.storeAccountMargin(accountMarginModel, ar -> {
                if (ar.succeeded()) {
                    asyncStore2.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncStore2.awaitSuccess(30000);

        this.checkCountInCollection(context, MongoPersistenceService.ACCOUNT_MARGIN_HISTORY_COLLECTION, 3408);
        this.checkCountInCollection(context, MongoPersistenceService.ACCOUNT_MARGIN_LATEST_COLLECTION, 1704);
        this.checkAccountMarginHistoryCollectionQuery(context);
        this.checkAccountMarginLatestCollectionQuery(context);
    }

    @Test
    public void testLiquiGroupMarginStore(TestContext context) throws IOException {
        Async asyncStore1 = context.async(2171);
        readTTSaveFile("liquiGroupMargin", 1, (header, gpbObject) -> {
            PrismaReports.LiquiGroupMargin liquiGroupMarginData = gpbObject.getExtension(PrismaReports.liquiGroupMargin);
            LiquiGroupMarginModel liquiGroupMarginModel = new LiquiGroupMarginModel(header, liquiGroupMarginData);
            persistenceProxy.storeLiquiGroupMargin(liquiGroupMarginModel, ar -> {
                if (ar.succeeded()) {
                    asyncStore1.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncStore1.awaitSuccess(30000);

        Async asyncStore2 = context.async(2171);
        readTTSaveFile("liquiGroupMargin", 2, (header, gpbObject) -> {
            PrismaReports.LiquiGroupMargin liquiGroupMarginData = gpbObject.getExtension(PrismaReports.liquiGroupMargin);
            LiquiGroupMarginModel liquiGroupMarginModel = new LiquiGroupMarginModel(header, liquiGroupMarginData);
            persistenceProxy.storeLiquiGroupMargin(liquiGroupMarginModel, ar -> {
                if (ar.succeeded()) {
                    asyncStore2.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncStore2.awaitSuccess(30000);

        this.checkCountInCollection(context, MongoPersistenceService.LIQUI_GROUP_MARGIN_HISTORY_COLLECTION, 4342);
        this.checkCountInCollection(context, MongoPersistenceService.LIQUI_GROUP_MARGIN_LATEST_COLLECTION, 2171);
        this.checkLiquiGroupMarginHistoryCollectionQuery(context);
        this.checkLiquiGroupMarginLatestCollectionQuery(context);
    }

    @Test
    public void testLiquiGroupSplitMarginStore(TestContext context) throws IOException {
        Async asyncStore1 = context.async(2472);
        readTTSaveFile("liquiGroupSplitMargin", 1, (header, gpbObject) -> {
            PrismaReports.LiquiGroupSplitMargin liquiGroupSplitMarginData = gpbObject.getExtension(PrismaReports.liquiGroupSplitMargin);
            LiquiGroupSplitMarginModel liquiGroupSplitMarginModel = new LiquiGroupSplitMarginModel(header, liquiGroupSplitMarginData);
            persistenceProxy.storeLiquiGroupSplitMargin(liquiGroupSplitMarginModel, ar -> {
                if (ar.succeeded()) {
                    asyncStore1.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncStore1.awaitSuccess(30000);

        Async asyncStore2 = context.async(2472);
        readTTSaveFile("liquiGroupSplitMargin", 2, (header, gpbObject) -> {
            PrismaReports.LiquiGroupSplitMargin liquiGroupSplitMarginData = gpbObject.getExtension(PrismaReports.liquiGroupSplitMargin);
            LiquiGroupSplitMarginModel liquiGroupSplitMarginModel = new LiquiGroupSplitMarginModel(header, liquiGroupSplitMarginData);
            persistenceProxy.storeLiquiGroupSplitMargin(liquiGroupSplitMarginModel, ar -> {
                if (ar.succeeded()) {
                    asyncStore2.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncStore2.awaitSuccess(30000);

        this.checkCountInCollection(context, MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_HISTORY_COLLECTION, 4944);
        this.checkCountInCollection(context, MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_LATEST_COLLECTION, 2472);
        this.checkLiquiGroupSplitMarginHistoryCollectionQuery(context);
        this.checkLiquiGroupSplitMarginLatestCollectionQuery(context);
    }

    @Test
    public void testPoolMarginStore(TestContext context) throws IOException {
        Async asyncFirstSnapshotStore = context.async(270);
        readTTSaveFile("poolMargin", 1, (header, gpbObject) -> {
            PrismaReports.PoolMargin poolMarginData = gpbObject.getExtension(PrismaReports.poolMargin);
            PoolMarginModel poolMarginModel = new PoolMarginModel(header, poolMarginData);
            persistenceProxy.storePoolMargin(poolMarginModel, ar -> {
                if (ar.succeeded()) {
                    asyncFirstSnapshotStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncFirstSnapshotStore.awaitSuccess(30000);
        Async asyncSecondSnapshotStore = context.async(270);
        readTTSaveFile("poolMargin", 2, (header, gpbObject) -> {
            PrismaReports.PoolMargin poolMarginData = gpbObject.getExtension(PrismaReports.poolMargin);
            PoolMarginModel poolMarginModel = new PoolMarginModel(header, poolMarginData);
            persistenceProxy.storePoolMargin(poolMarginModel, ar -> {
                if (ar.succeeded()) {
                    asyncSecondSnapshotStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncSecondSnapshotStore.awaitSuccess(30000);
        this.checkCountInCollection(context, MongoPersistenceService.POOL_MARGIN_HISTORY_COLLECTION, 540);
        this.checkCountInCollection(context, MongoPersistenceService.POOL_MARGIN_LATEST_COLLECTION, 270);
        this.checkPoolMarginHistoryCollectionQuery(context);
        this.checkPoolMarginLatestCollectionQuery(context);
    }

    @Test
    public void testPositionReportStore(TestContext context) throws IOException {
        Async asyncFirstSnapshotStore = context.async(3596);
        readTTSaveFile("positionReport", 1, (header, gpbObject) -> {
            PrismaReports.PositionReport positionReportData = gpbObject.getExtension(PrismaReports.positionReport);
            PositionReportModel positionReportModel = new PositionReportModel(header, positionReportData);
            persistenceProxy.storePositionReport(positionReportModel, ar -> {
                if (ar.succeeded()) {
                    asyncFirstSnapshotStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncFirstSnapshotStore.awaitSuccess(30000);
        Async asyncSecondSnapshotStore = context.async(3596);
        readTTSaveFile("positionReport", 2, (header, gpbObject) -> {
            PrismaReports.PositionReport positionReportData = gpbObject.getExtension(PrismaReports.positionReport);
            PositionReportModel positionReportModel = new PositionReportModel(header, positionReportData);
            persistenceProxy.storePositionReport(positionReportModel, ar -> {
                if (ar.succeeded()) {
                    asyncSecondSnapshotStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncSecondSnapshotStore.awaitSuccess(30000);
        this.checkCountInCollection(context, MongoPersistenceService.POSITION_REPORT_HISTORY_COLLECTION, 7192);
        this.checkCountInCollection(context, MongoPersistenceService.POSITION_REPORT_LATEST_COLLECTION, 3596);
        this.checkPositionReportHistoryCollectionQuery(context);
        this.checkPositionReportLatestCollectionQuery(context);
    }

    @Test
    public void testRiskLimitUtilizationStore(TestContext context) throws IOException {
        Async asyncFirstSnapshotStore = context.async(6);
        readTTSaveFile("riskLimitUtilization", 1, (header, gpbObject) -> {
            PrismaReports.RiskLimitUtilization data = gpbObject.getExtension(PrismaReports.riskLimitUtilization);
            RiskLimitUtilizationModel model = new RiskLimitUtilizationModel(header, data);
            persistenceProxy.storeRiskLimitUtilization(model, ar -> {
                if (ar.succeeded()) {
                    asyncFirstSnapshotStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncFirstSnapshotStore.awaitSuccess(30000);
        Async asyncSecondSnapshotStore = context.async(6);
        readTTSaveFile("riskLimitUtilization", 2, (header, gpbObject) -> {
            PrismaReports.RiskLimitUtilization date = gpbObject.getExtension(PrismaReports.riskLimitUtilization);
            RiskLimitUtilizationModel model = new RiskLimitUtilizationModel(header, date);
            persistenceProxy.storeRiskLimitUtilization(model, ar -> {
                if (ar.succeeded()) {
                    asyncSecondSnapshotStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncSecondSnapshotStore.awaitSuccess(30000);
        this.checkCountInCollection(context, MongoPersistenceService.RISK_LIMIT_UTILIZATION_HISTORY_COLLECTION, 12);
        this.checkCountInCollection(context, MongoPersistenceService.RISK_LIMIT_UTILIZATION_LATEST_COLLECTION, 6);
        this.checkRiskLimitUtilizationHistoryCollectionQuery(context);
        this.checkRiskLimitUtilizationLatestCollectionQuery(context);
    }

    /**
     * @param folderName "accountMargin", "liquiGroupMargin" or "poolMargin"
     * @param ttsaveNo 1 for the first snapshot, etc.
     */
    private void readTTSaveFile(String folderName, int ttsaveNo, BiConsumer<? super PrismaReports.PrismaHeader, ? super ObjectList.GPBObject> consumer) {
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        PrismaReports.registerAllExtensions(registry);
        String path = String.format("%s/%03d.bin", MainVerticleIT.class.getResource(folderName).getPath(), ttsaveNo);
        try {
            byte[] gpbBytes = Files.readAllBytes(Paths.get(path));
            ObjectList.GPBObjectList gpbObjectList = ObjectList.GPBObjectList.parseFrom(gpbBytes, registry);
            PrismaReports.PrismaHeader header = gpbObjectList.getHeader().getExtension(PrismaReports.prismaHeader);
            gpbObjectList.getItemList().forEach(gpbObject -> consumer.accept(header, gpbObject));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void checkCountInCollection(TestContext context, String collection, long count) {
        Async asyncHistoryCount = context.async();
        MongoPersistenceServiceIT.mongoClient.count(collection, new JsonObject(), ar -> {
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
        /* You can use this query to paste it directly into MongoDB shell, this is
           what this test case expects:
           db.AccountMargin.find({
               clearer: "BERFR",
               member: "BERFR",
               account: "A5",
               marginCurrency: "EUR"
           }).sort({snapshotID: 1}).pretty()
        */
        JsonObject param = new JsonObject();
        param.put("clearer", "BERFR");
        param.put("member", "BERFR");
        param.put("account", "A5");
        param.put("marginCurrency", "EUR");

        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.find(MongoPersistenceService.ACCOUNT_MARGIN_HISTORY_COLLECTION, param, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(2, ar.result().size());
                JsonObject result = ar.result().get(0);

                context.assertEquals(10, result.getInteger("snapshotID"));
                context.assertEquals(20091215, result.getInteger("businessDate"));
                context.assertEquals(new JsonObject().put("$date", "2017-02-15T15:27:02.43Z"), result.getJsonObject("timestamp"));
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
        /* You can use this query to paste it directly into MongoDB shell, this is
           what this test case expects:
           db.AccountMargin.latest.find({
               clearer: "BERFR",
               member: "BERFR",
               account: "A5",
               marginCurrency: "EUR"
           }).pretty()
        */
        JsonObject param = new JsonObject();
        param.put("clearer", "BERFR");
        param.put("member", "BERFR");
        param.put("account", "A5");
        param.put("marginCurrency", "EUR");

        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.find(MongoPersistenceService.ACCOUNT_MARGIN_LATEST_COLLECTION, param, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(1, ar.result().size());
                JsonObject result = ar.result().get(0);

                context.assertEquals(11, result.getInteger("snapshotID"));
                context.assertEquals(20091215, result.getInteger("businessDate"));
                context.assertEquals(new JsonObject().put("$date", "2017-02-15T15:28:50.03Z"), result.getJsonObject("timestamp"));
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
        /* You can use this query to paste it directly into MongoDB shell, this is
           what this test case expects:
           db.LiquiGroupMargin.find({
               clearer: "ABCFR",
               member: "ABCFR",
               account: "PP",
               marginClass: "ECC01",
               marginCurrency: "EUR"
           }).sort({snapshotID: 1}).pretty()
        */
        JsonObject param = new JsonObject();
        param.put("clearer", "ABCFR");
        param.put("member", "ABCFR");
        param.put("account", "PP");
        param.put("marginClass", "ECC01");
        param.put("marginCurrency", "EUR");

        FindOptions findOptions = new FindOptions()
                .setSort(new JsonObject().put("snapshotID", 1));

        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.findWithOptions(MongoPersistenceService.LIQUI_GROUP_MARGIN_HISTORY_COLLECTION, param, findOptions, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(2, ar.result().size());
                JsonObject result = ar.result().get(0);

                context.assertEquals(10, result.getInteger("snapshotID"));
                context.assertEquals(20091215, result.getInteger("businessDate"));
                context.assertEquals(new JsonObject().put("$date", "2017-02-15T15:27:02.43Z"), result.getJsonObject("timestamp"));
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
        /* You can use this query to paste it directly into MongoDB shell, this is
           what this test case expects:
           db.LiquiGroupMargin.latest.find({
               clearer: "ABCFR",
               member: "ABCFR",
               account: "PP",
               marginClass: "ECC01",
               marginCurrency: "EUR"
           }).pretty()
        */
        JsonObject param = new JsonObject();
        param.put("clearer", "ABCFR");
        param.put("member", "ABCFR");
        param.put("account", "PP");
        param.put("marginClass", "ECC01");
        param.put("marginCurrency", "EUR");

        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.find(MongoPersistenceService.LIQUI_GROUP_MARGIN_LATEST_COLLECTION, param, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(1, ar.result().size());
                JsonObject result = ar.result().get(0);

                context.assertEquals(11, result.getInteger("snapshotID"));
                context.assertEquals(20091215, result.getInteger("businessDate"));
                context.assertEquals(new JsonObject().put("$date", "2017-02-15T15:28:50.03Z"), result.getJsonObject("timestamp"));
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

    private void checkLiquiGroupSplitMarginHistoryCollectionQuery(TestContext context) {
        /* You can use this query to paste it directly into MongoDB shell, this is
           what this test case expects:
           db.LiquiGroupSplitMargin.find({
               clearer: "USJPM",
               member: "USJPM",
               account: "PP",
               liquidationGroup: "PFI02",
               liquidationGroupSplit: "PFI02_HP2_T3-99999",
               marginCurrency: "EUR"
           }).sort({snapshotID: 1}).pretty()
        */
        JsonObject param = new JsonObject();
        param.put("clearer", "USJPM");
        param.put("member", "USJPM");
        param.put("account", "PP");
        param.put("liquidationGroup", "PFI02");
        param.put("liquidationGroupSplit", "PFI02_HP2_T3-99999");
        param.put("marginCurrency", "EUR");

        FindOptions findOptions = new FindOptions()
                .setSort(new JsonObject().put("snapshotID", 1));

        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.findWithOptions(MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_HISTORY_COLLECTION, param, findOptions, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(2, ar.result().size());
                JsonObject result = ar.result().get(0);

                context.assertEquals(15, result.getInteger("snapshotID"));
                context.assertEquals(20091215, result.getInteger("businessDate"));
                context.assertEquals(new JsonObject().put("$date", "2017-02-21T11:43:34.791Z"), result.getJsonObject("timestamp"));
                context.assertEquals("USJPM", result.getString("clearer"));
                context.assertEquals("USJPM", result.getString("member"));
                context.assertEquals("PP", result.getString("account"));
                context.assertEquals("PFI02", result.getString("liquidationGroup"));
                context.assertEquals("PFI02_HP2_T3-99999", result.getString("liquidationGroupSplit"));
                context.assertEquals("EUR", result.getString("marginCurrency"));
                context.assertEquals(0.0, result.getDouble("premiumMargin"));
                context.assertEquals(2.7548216040760565E8, result.getDouble("marketRisk"));
                context.assertEquals(3.690967426538666E7, result.getDouble("liquRisk"));
                context.assertEquals(0.0, result.getDouble("longOptionCredit"));
                context.assertEquals(4.86621581017E8, result.getDouble("variationPremiumPayment"));
                asyncQuery.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncQuery.awaitSuccess(5000);
    }

    private void checkLiquiGroupSplitMarginLatestCollectionQuery(TestContext context) {
        /* You can use this query to paste it directly into MongoDB shell, this is
           what this test case expects:
           db.LiquiGroupSplitMargin.latest.find({
               clearer: "USJPM",
               member: "USJPM",
               account: "PP",
               liquidationGroup: "PFI02",
               liquidationGroupSplit: "PFI02_HP2_T3-99999",
               marginCurrency: "EUR"
           }).pretty()
        */
        JsonObject param = new JsonObject();
        param.put("clearer", "USJPM");
        param.put("member", "USJPM");
        param.put("account", "PP");
        param.put("liquidationGroup", "PFI02");
        param.put("liquidationGroupSplit", "PFI02_HP2_T3-99999");
        param.put("marginCurrency", "EUR");


        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.find(MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_LATEST_COLLECTION, param, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(1, ar.result().size());
                JsonObject result = ar.result().get(0);

                context.assertEquals(16, result.getInteger("snapshotID"));
                context.assertEquals(20091215, result.getInteger("businessDate"));
                context.assertEquals(new JsonObject().put("$date", "2017-02-21T11:44:56.396Z"), result.getJsonObject("timestamp"));
                context.assertEquals("USJPM", result.getString("clearer"));
                context.assertEquals("USJPM", result.getString("member"));
                context.assertEquals("PP", result.getString("account"));
                context.assertEquals("PFI02", result.getString("liquidationGroup"));
                context.assertEquals("PFI02_HP2_T3-99999", result.getString("liquidationGroupSplit"));
                context.assertEquals("EUR", result.getString("marginCurrency"));
                context.assertEquals(0.0, result.getDouble("premiumMargin"));
                context.assertEquals(2.7548216040760565E8, result.getDouble("marketRisk"));
                context.assertEquals(3.690967426538666E7, result.getDouble("liquRisk"));
                context.assertEquals(0.0, result.getDouble("longOptionCredit"));
                context.assertEquals(4.86621581017E8, result.getDouble("variationPremiumPayment"));
                asyncQuery.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncQuery.awaitSuccess(5000);
    }

    private void checkPoolMarginHistoryCollectionQuery(TestContext context) {
        /* You can use this query to paste it directly into MongoDB shell, this is
           what this test case expects:
           db.PoolMargin.find({
               clearer: "USWFC",
               pool: "default",
               marginCurrency: "CHF"
           }).sort({snapshotID: 1}).pretty()
        */
        JsonObject param = new JsonObject()
                .put("clearer", "USWFC")
                .put("pool", "default")
                .put("marginCurrency", "CHF");
        FindOptions findOptions = new FindOptions()
                .setSort(new JsonObject().put("snapshotID", 1));

        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.findWithOptions(MongoPersistenceService.POOL_MARGIN_HISTORY_COLLECTION, param, findOptions, ar -> {
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
        /* You can use this query to paste it directly into MongoDB shell, this is
           what this test case expects:
           db.PoolMargin.latest.find({
               clearer: "USWFC",
               pool: "default",
               marginCurrency: "CHF"
           }).pretty()
        */
        JsonObject param = new JsonObject()
                .put("clearer", "USWFC")
                .put("pool", "default")
                .put("marginCurrency", "CHF");

        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.find(MongoPersistenceService.POOL_MARGIN_LATEST_COLLECTION, param, ar -> {
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

    private void checkPositionReportHistoryCollectionQuery(TestContext context) {
        /* You can use this query to paste it directly into MongoDB shell, this is
           what this test case expects:
           db.PositionReport.find({
                clearer: "BERFR",
                member: "BERFR",
                account: "PP",
                liquidationGroup: "PEQ01",
                liquidationGroupSplit: "PEQ01_Basic",
                product: "ALV",
                callPut: "P",
                contractYear: 2010,
                contractMonth: 2,
                expiryDay: 0,
                exercisePrice: 170,
                version: "0",
                flexContractSymbol: ""
           }).sort({snapshotID: 1}).pretty()
        */
        JsonObject param = new JsonObject()
                .put("clearer", "BERFR")
                .put("member", "BERFR")
                .put("account", "PP")
                .put("liquidationGroup", "PEQ01")
                .put("liquidationGroupSplit", "PEQ01_Basic")
                .put("product", "ALV")
                .put("callPut", "P")
                .put("contractYear", 2010)
                .put("contractMonth", 2)
                .put("expiryDay", 0)
                .put("exercisePrice", 170)
                .put("version", "0")
                .put("flexContractSymbol", "");

        FindOptions findOptions = new FindOptions()
                .setSort(new JsonObject().put("snapshotID", 1));

        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.findWithOptions(MongoPersistenceService.POSITION_REPORT_HISTORY_COLLECTION, param, findOptions, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(2, ar.result().size());
                JsonObject result1 = ar.result().get(0);

                context.assertEquals(15, result1.getInteger("snapshotID"));
                context.assertEquals(20091215, result1.getInteger("businessDate"));
                context.assertEquals(new JsonObject().put("$date", "2017-02-21T11:43:34.791Z"), result1.getJsonObject("timestamp"));
                context.assertEquals("BERFR", result1.getString("clearer"));
                context.assertEquals("BERFR", result1.getString("member"));
                context.assertEquals("PP", result1.getString("account"));
                context.assertEquals("PEQ01", result1.getString("liquidationGroup"));
                context.assertEquals("PEQ01_Basic", result1.getString("liquidationGroupSplit"));
                context.assertEquals("ALV", result1.getString("product"));
                context.assertEquals("P", result1.getString("callPut"));
                context.assertEquals(2010, result1.getInteger("contractYear"));
                context.assertEquals(2, result1.getInteger("contractMonth"));
                context.assertEquals(0, result1.getInteger("expiryDay"));
                context.assertEquals(170.0, result1.getDouble("exercisePrice"));
                context.assertEquals("0", result1.getString("version"));
                context.assertEquals("", result1.getString("flexContractSymbol"));
                context.assertEquals(-1943.0, result1.getDouble("netQuantityLs"));
                context.assertEquals(0.0, result1.getDouble("netQuantityEa"));
                context.assertEquals("EUR", result1.getString("clearingCurrency"));
                context.assertEquals(22.539110869169235, result1.getDouble("mVar"));
                context.assertEquals(21.725328222930674, result1.getDouble("compVar"));
                context.assertEquals(0.813782657069325, result1.getDouble("compCorrelationBreak"));
                context.assertEquals(0.0060874443941714195, result1.getDouble("compCompressionError"));
                context.assertEquals(6.287057332082233, result1.getDouble("compLiquidityAddOn"));
                context.assertEquals(-28.832255656476406, result1.getDouble("compLongOptionCredit"));
                context.assertEquals("EUR", result1.getString("productCurrency"));
                context.assertInRange(0.0, result1.getDouble("variationPremiumPayment"), DOUBLE_DELTA);
                context.assertInRange(0.0, result1.getDouble("premiumMargin"), DOUBLE_DELTA);
                context.assertEquals(0.04391331213559379, result1.getDouble("normalizedDelta"));
                context.assertEquals(-0.0021834196488993104, result1.getDouble("normalizedGamma"));
                context.assertEquals(-0.002614335157912494, result1.getDouble("normalizedVega"));
                context.assertEquals(0.00008193269784691068, result1.getDouble("normalizedRho"));
                context.assertEquals(0.0004722838437817084, result1.getDouble("normalizedTheta"));
                context.assertEquals("ALV", result1.getString("underlying"));

                asyncQuery.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncQuery.awaitSuccess(5000);
    }

    private void checkPositionReportLatestCollectionQuery(TestContext context) {
        /* You can use this query to paste it directly into MongoDB shell, this is
           what this test case expects:
           db.PositionReport.latest.find({
                clearer: "BERFR",
                member: "BERFR",
                account: "PP",
                liquidationGroup: "PEQ01",
                liquidationGroupSplit: "PEQ01_Basic",
                product: "ALV",
                callPut: "P",
                contractYear: 2010,
                contractMonth: 2,
                expiryDay: 0,
                exercisePrice: 170,
                version: "0",
                flexContractSymbol: ""
           }).pretty()
        */
        JsonObject param = new JsonObject()
                .put("clearer", "BERFR")
                .put("member", "BERFR")
                .put("account", "PP")
                .put("liquidationGroup", "PEQ01")
                .put("liquidationGroupSplit", "PEQ01_Basic")
                .put("product", "ALV")
                .put("callPut", "P")
                .put("contractYear", 2010)
                .put("contractMonth", 2)
                .put("expiryDay", 0)
                .put("exercisePrice", 170)
                .put("version", "0")
                .put("flexContractSymbol", "");

        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.find(MongoPersistenceService.POSITION_REPORT_LATEST_COLLECTION, param, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(1, ar.result().size());
                JsonObject result1 = ar.result().get(0);

                context.assertEquals(16, result1.getInteger("snapshotID"));
                context.assertEquals(20091215, result1.getInteger("businessDate"));
                context.assertEquals(new JsonObject().put("$date", "2017-02-21T11:44:56.396Z"), result1.getJsonObject("timestamp"));
                context.assertEquals("BERFR", result1.getString("clearer"));
                context.assertEquals("BERFR", result1.getString("member"));
                context.assertEquals("PP", result1.getString("account"));
                context.assertEquals("PEQ01", result1.getString("liquidationGroup"));
                context.assertEquals("PEQ01_Basic", result1.getString("liquidationGroupSplit"));
                context.assertEquals("ALV", result1.getString("product"));
                context.assertEquals("P", result1.getString("callPut"));
                context.assertEquals(2010, result1.getInteger("contractYear"));
                context.assertEquals(2, result1.getInteger("contractMonth"));
                context.assertEquals(0, result1.getInteger("expiryDay"));
                context.assertEquals(170.0, result1.getDouble("exercisePrice"));
                context.assertEquals("0", result1.getString("version"));
                context.assertEquals("", result1.getString("flexContractSymbol"));
                context.assertEquals(-1943.0, result1.getDouble("netQuantityLs"));
                context.assertEquals(0.0, result1.getDouble("netQuantityEa"));
                context.assertEquals("EUR", result1.getString("clearingCurrency"));
                context.assertEquals(22.539110869169235, result1.getDouble("mVar"));
                context.assertEquals(21.725328222930678, result1.getDouble("compVar"));
                context.assertEquals(0.8137826570693243, result1.getDouble("compCorrelationBreak"));
                context.assertEquals(0.0060874443941714195, result1.getDouble("compCompressionError"));
                context.assertEquals(6.287057332082231, result1.getDouble("compLiquidityAddOn"));
                context.assertEquals(-28.832255656476406, result1.getDouble("compLongOptionCredit"));
                context.assertEquals("EUR", result1.getString("productCurrency"));
                context.assertInRange(0.0, result1.getDouble("variationPremiumPayment"), DOUBLE_DELTA);
                context.assertInRange(0.0, result1.getDouble("premiumMargin"), DOUBLE_DELTA);
                context.assertEquals(0.04391331213559379, result1.getDouble("normalizedDelta"));
                context.assertEquals(-0.0021834196488993104, result1.getDouble("normalizedGamma"));
                context.assertEquals(-0.002614335157912494, result1.getDouble("normalizedVega"));
                context.assertEquals(0.00008193269784691068, result1.getDouble("normalizedRho"));
                context.assertEquals(0.0004722838437817084, result1.getDouble("normalizedTheta"));
                context.assertEquals("ALV", result1.getString("underlying"));

                asyncQuery.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncQuery.awaitSuccess(5000);
    }

    private void checkRiskLimitUtilizationHistoryCollectionQuery(TestContext context) {
        /* You can use this query to paste it directly into MongoDB shell, this is
           what this test case expects:
           db.RiskLimitUtilization.find({
                clearer : "FULCC",
                member : "MALFR",
                maintainer : "MALFR",
                limitType : "TMR",
           }).sort({snapshotID: 1}).pretty()
        */
        JsonObject param = new JsonObject()
                .put("clearer", "FULCC")
                .put("member", "MALFR")
                .put("maintainer", "MALFR")
                .put("limitType", "TMR");

        FindOptions findOptions = new FindOptions()
                .setSort(new JsonObject().put("snapshotID", 1));

        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.findWithOptions(MongoPersistenceService.RISK_LIMIT_UTILIZATION_HISTORY_COLLECTION, param, findOptions, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(2, ar.result().size());
                JsonObject result1 = ar.result().get(0);

                context.assertEquals(15, result1.getInteger("snapshotID"));
                context.assertEquals(20091215, result1.getInteger("businessDate"));
                context.assertEquals(new JsonObject().put("$date", "2017-02-21T11:43:34.791Z"), result1.getJsonObject("timestamp"));
                context.assertEquals("FULCC", result1.getString("clearer"));
                context.assertEquals("MALFR", result1.getString("member"));
                context.assertEquals("MALFR", result1.getString("maintainer"));
                context.assertEquals("TMR", result1.getString("limitType"));
                context.assertEquals(8862049569.447277, result1.getDouble("utilization"));
                context.assertEquals(1010020.0, result1.getDouble("warningLevel"));
                context.assertEquals(0.0, result1.getDouble("throttleLevel"));
                context.assertEquals(1010020.0, result1.getDouble("rejectLevel"));

                asyncQuery.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncQuery.awaitSuccess(5000);
    }

    private void checkRiskLimitUtilizationLatestCollectionQuery(TestContext context) {
        /* You can use this query to paste it directly into MongoDB shell, this is
           what this test case expects:
           db.RiskLimitUtilization.find({
                clearer : "FULCC",
                member : "MALFR",
                maintainer : "MALFR",
                limitType : "TMR",
           }).sort({snapshotID: 1}).pretty()
        */
        JsonObject param = new JsonObject()
                .put("clearer", "FULCC")
                .put("member", "MALFR")
                .put("maintainer", "MALFR")
                .put("limitType", "TMR");

        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.find(MongoPersistenceService.RISK_LIMIT_UTILIZATION_LATEST_COLLECTION, param, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(1, ar.result().size());
                JsonObject result1 = ar.result().get(0);

                context.assertEquals(16, result1.getInteger("snapshotID"));
                context.assertEquals(20091215, result1.getInteger("businessDate"));
                context.assertEquals(new JsonObject().put("$date", "2017-02-21T11:44:56.396Z"), result1.getJsonObject("timestamp"));
                context.assertEquals("FULCC", result1.getString("clearer"));
                context.assertEquals("MALFR", result1.getString("member"));
                context.assertEquals("MALFR", result1.getString("maintainer"));
                context.assertEquals("TMR", result1.getString("limitType"));
                context.assertEquals(8862049569.447277, result1.getDouble("utilization"));
                context.assertEquals(1010020.0, result1.getDouble("warningLevel"));
                context.assertEquals(0.0, result1.getDouble("throttleLevel"));
                context.assertEquals(1010020.0, result1.getDouble("rejectLevel"));

                asyncQuery.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncQuery.awaitSuccess(5000);
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        ProxyHelper.unregisterService(MongoPersistenceServiceIT.persistenceServiceConsumer);
        MongoPersistenceServiceIT.vertx.close(context.asyncAssertSuccess());
    }

}
