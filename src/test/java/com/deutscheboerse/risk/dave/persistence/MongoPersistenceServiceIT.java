 package com.deutscheboerse.risk.dave.persistence;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.deutscheboerse.risk.dave.BaseTest;
import com.deutscheboerse.risk.dave.log.TestAppender;
import com.deutscheboerse.risk.dave.model.*;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.serviceproxy.ProxyHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

@RunWith(VertxUnitRunner.class)
public class MongoPersistenceServiceIT extends BaseTest {
    private static final TestAppender testAppender = TestAppender.getAppender(MongoPersistenceService.class);
    private static final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private static Vertx vertx;
    private static MongoClient mongoClient;
    private static PersistenceService persistenceProxy;

    @BeforeClass
    public static void setUp(TestContext context) {
        MongoPersistenceServiceIT.vertx = Vertx.vertx();
        JsonObject config = BaseTest.getMongoConfig();
        JsonObject mongoConfig = BaseTest.getMongoClientConfig(config);

        MongoPersistenceServiceIT.mongoClient = MongoClient.createShared(MongoPersistenceServiceIT.vertx, mongoConfig);

        ProxyHelper.registerService(PersistenceService.class, vertx, new MongoPersistenceService(vertx, mongoClient), PersistenceService.SERVICE_ADDRESS);
        MongoPersistenceServiceIT.persistenceProxy = ProxyHelper.createProxy(PersistenceService.class, vertx, PersistenceService.SERVICE_ADDRESS);
        MongoPersistenceServiceIT.persistenceProxy.initialize(context.asyncAssertSuccess());

        rootLogger.addAppender(testAppender);
    }

    @Test
    public void checkCollectionsExist(TestContext context) {
        List<String> requiredCollections = new ArrayList<>();
        requiredCollections.addAll(MongoPersistenceService.getRequiredCollections());
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
        final Async async = context.async(MongoPersistenceService.getRequiredCollections().size());
        BiConsumer<String, JsonObject> indexCheck = (collectionName, expectedIndex) ->
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

        indexCheck.accept(MongoPersistenceService.ACCOUNT_MARGIN_COLLECTION, MongoPersistenceService.getUniqueIndex(new AccountMarginModel()));
        indexCheck.accept(MongoPersistenceService.LIQUI_GROUP_MARGIN_COLLECTION, MongoPersistenceService.getUniqueIndex(new LiquiGroupMarginModel()));
        indexCheck.accept(MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_COLLECTION, MongoPersistenceService.getUniqueIndex(new LiquiGroupSplitMarginModel()));
        indexCheck.accept(MongoPersistenceService.POOL_MARGIN_COLLECTION, MongoPersistenceService.getUniqueIndex(new PoolMarginModel()));
        indexCheck.accept(MongoPersistenceService.POSITION_REPORT_COLLECTION, MongoPersistenceService.getUniqueIndex(new PositionReportModel()));
        indexCheck.accept(MongoPersistenceService.RISK_LIMIT_UTILIZATION_COLLECTION, MongoPersistenceService.getUniqueIndex(new RiskLimitUtilizationModel()));
    }

    @Test
    public void testGetCollectionsError(TestContext context) throws InterruptedException {
        this.testErrorInInitialize(context, "getCollections", "Failed to get collection list");
    }

    @Test
    public void testCreateCollectionError(TestContext context) throws InterruptedException {
        this.testErrorInInitialize(context, "createCollection", "Failed to add all collections");
    }

    @Test
    public void testCreateIndexWithOptionsError(TestContext context) throws InterruptedException {
        this.testErrorInInitialize(context, "createIndexWithOptions", "Failed to create all needed indexes in Mongo");
    }

    @Test
    public void testConnectionStatusBackOnline(TestContext context) throws InterruptedException {
        JsonObject proxyConfig = new JsonObject().put("functionsToFail", new JsonArray().add("updateCollectionWithOptions"));

        final PersistenceService persistenceErrorProxy = getPersistenceErrorProxy(proxyConfig);
        persistenceErrorProxy.initialize(context.asyncAssertSuccess());

        final AccountMarginModel model = new AccountMarginModel(new JsonObject().put("timestamp", 0L));

        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        persistenceErrorProxy.storeAccountMargin(model, context.asyncAssertFailure());
        testAppender.waitForMessageContains(Level.INFO, "Back online");
        testAppender.stop();
        rootLogger.addAppender(stdout);

        persistenceErrorProxy.close();
    }

    @Test
    public void testConnectionStatusError(TestContext context) throws InterruptedException {
        JsonObject proxyConfig = new JsonObject().put("functionsToFail", new JsonArray().add("updateCollectionWithOptions").add("runCommand"));

        final PersistenceService persistenceErrorProxy = getPersistenceErrorProxy(proxyConfig);
        persistenceErrorProxy.initialize(context.asyncAssertSuccess());

        final AccountMarginModel model = new AccountMarginModel(new JsonObject().put("timestamp", 0L));

        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        persistenceErrorProxy.storeAccountMargin(model, context.asyncAssertFailure());
        testAppender.waitForMessageContains(Level.ERROR, "Still disconnected");
        testAppender.stop();
        rootLogger.addAppender(stdout);

        persistenceErrorProxy.close();
    }

    @Test
    public void testAccountMarginStore(TestContext context) throws IOException {
        int firstMsgCount = DataHelper.getJsonObjectCount("accountMargin", 1);
        Async asyncStore1 = context.async(firstMsgCount);
        DataHelper.readTTSaveFile("accountMargin", 1, (json) -> {
            AccountMarginModel accountMarginModel = new AccountMarginModel(json);
            persistenceProxy.storeAccountMargin(accountMarginModel, ar -> {
                if (ar.succeeded()) {
                    asyncStore1.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncStore1.awaitSuccess(30000);

        int secondMsgCount = DataHelper.getJsonObjectCount("accountMargin", 2);
        Async asyncStore2 = context.async(secondMsgCount);
        DataHelper.readTTSaveFile("accountMargin", 2, (json) -> {
            AccountMarginModel accountMarginModel = new AccountMarginModel(json);
            persistenceProxy.storeAccountMargin(accountMarginModel, ar -> {
                if (ar.succeeded()) {
                    asyncStore2.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncStore2.awaitSuccess(30000);

        this.checkCountInCollection(context, MongoPersistenceService.ACCOUNT_MARGIN_COLLECTION, firstMsgCount);
        this.checkAccountMarginCollectionQuery(context);
    }

    @Test
    public void testLiquiGroupMarginStore(TestContext context) throws IOException {
        int firstMsgCount = DataHelper.getJsonObjectCount("liquiGroupMargin", 1);
        Async asyncStore1 = context.async(firstMsgCount);
        DataHelper.readTTSaveFile("liquiGroupMargin", 1, (json) -> {
            LiquiGroupMarginModel liquiGroupMarginModel = new LiquiGroupMarginModel(json);
            persistenceProxy.storeLiquiGroupMargin(liquiGroupMarginModel, ar -> {
                if (ar.succeeded()) {
                    asyncStore1.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncStore1.awaitSuccess(30000);

        int secondMsgCount = DataHelper.getJsonObjectCount("liquiGroupMargin", 2);
        Async asyncStore2 = context.async(secondMsgCount);
        DataHelper.readTTSaveFile("liquiGroupMargin", 2, (json) -> {
            LiquiGroupMarginModel liquiGroupMarginModel = new LiquiGroupMarginModel(json);
            persistenceProxy.storeLiquiGroupMargin(liquiGroupMarginModel, ar -> {
                if (ar.succeeded()) {
                    asyncStore2.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncStore2.awaitSuccess(30000);

        this.checkCountInCollection(context, MongoPersistenceService.LIQUI_GROUP_MARGIN_COLLECTION, firstMsgCount);
        this.checkLiquiGroupMarginCollectionQuery(context);
    }

    @Test
    public void testLiquiGroupSplitMarginStore(TestContext context) throws IOException {
        int firstMsgCount = DataHelper.getJsonObjectCount("liquiGroupSplitMargin", 1);
        Async asyncStore1 = context.async(firstMsgCount);
        DataHelper.readTTSaveFile("liquiGroupSplitMargin", 1, (json) -> {
            LiquiGroupSplitMarginModel liquiGroupSplitMarginModel = new LiquiGroupSplitMarginModel(json);
            persistenceProxy.storeLiquiGroupSplitMargin(liquiGroupSplitMarginModel, ar -> {
                if (ar.succeeded()) {
                    asyncStore1.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncStore1.awaitSuccess(30000);

        int secondMsgCount = DataHelper.getJsonObjectCount("liquiGroupSplitMargin", 2);
        Async asyncStore2 = context.async(secondMsgCount);
        DataHelper.readTTSaveFile("liquiGroupSplitMargin", 2, (json) -> {
            LiquiGroupSplitMarginModel liquiGroupSplitMarginModel = new LiquiGroupSplitMarginModel(json);
            persistenceProxy.storeLiquiGroupSplitMargin(liquiGroupSplitMarginModel, ar -> {
                if (ar.succeeded()) {
                    asyncStore2.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncStore2.awaitSuccess(30000);

        this.checkCountInCollection(context, MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_COLLECTION, firstMsgCount);
        this.checkLiquiGroupSplitMarginCollectionQuery(context);
    }

    @Test
    public void testPoolMarginStore(TestContext context) throws IOException {
        int firstMsgCount = DataHelper.getJsonObjectCount("poolMargin", 1);
        Async asyncFirstSnapshotStore = context.async(firstMsgCount);
        DataHelper.readTTSaveFile("poolMargin", 1, (json) -> {
            PoolMarginModel poolMarginModel = new PoolMarginModel(json);
            persistenceProxy.storePoolMargin(poolMarginModel, ar -> {
                if (ar.succeeded()) {
                    asyncFirstSnapshotStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncFirstSnapshotStore.awaitSuccess(30000);

        int secondMsgCount = DataHelper.getJsonObjectCount("poolMargin", 2);
        Async asyncSecondSnapshotStore = context.async(secondMsgCount);
        DataHelper.readTTSaveFile("poolMargin", 2, (json) -> {
            PoolMarginModel poolMarginModel = new PoolMarginModel(json);
            persistenceProxy.storePoolMargin(poolMarginModel, ar -> {
                if (ar.succeeded()) {
                    asyncSecondSnapshotStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncSecondSnapshotStore.awaitSuccess(30000);
        this.checkCountInCollection(context, MongoPersistenceService.POOL_MARGIN_COLLECTION, firstMsgCount);
        this.checkPoolMarginCollectionQuery(context);
    }

    @Test
    public void testPositionReportStore(TestContext context) throws IOException {
        int firstMsgCount = DataHelper.getJsonObjectCount("positionReport", 1);
        Async asyncFirstSnapshotStore = context.async(firstMsgCount);
        DataHelper.readTTSaveFile("positionReport", 1, (json) -> {
            PositionReportModel positionReportModel = new PositionReportModel(json);
            persistenceProxy.storePositionReport(positionReportModel, ar -> {
                if (ar.succeeded()) {
                    asyncFirstSnapshotStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncFirstSnapshotStore.awaitSuccess(30000);
        int secondMsgCount = DataHelper.getJsonObjectCount("positionReport", 2);
        Async asyncSecondSnapshotStore = context.async(secondMsgCount);
        DataHelper.readTTSaveFile("positionReport", 2, (json) -> {
            PositionReportModel positionReportModel = new PositionReportModel(json);
            persistenceProxy.storePositionReport(positionReportModel, ar -> {
                if (ar.succeeded()) {
                    asyncSecondSnapshotStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncSecondSnapshotStore.awaitSuccess(30000);
        this.checkCountInCollection(context, MongoPersistenceService.POSITION_REPORT_COLLECTION, firstMsgCount );
        this.checkPositionReportCollectionQuery(context);
    }

    @Test
    public void testRiskLimitUtilizationStore(TestContext context) throws IOException {
        int firstMsgCount = DataHelper.getJsonObjectCount("riskLimitUtilization", 1);
        Async asyncFirstSnapshotStore = context.async(firstMsgCount);
        DataHelper.readTTSaveFile("riskLimitUtilization", 1, (json) -> {
            RiskLimitUtilizationModel model = new RiskLimitUtilizationModel(json);
            persistenceProxy.storeRiskLimitUtilization(model, ar -> {
                if (ar.succeeded()) {
                    asyncFirstSnapshotStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncFirstSnapshotStore.awaitSuccess(30000);
        int secondMsgCount = DataHelper.getJsonObjectCount("riskLimitUtilization", 2);
        Async asyncSecondSnapshotStore = context.async(secondMsgCount);
        DataHelper.readTTSaveFile("riskLimitUtilization", 2, (json) -> {
            RiskLimitUtilizationModel model = new RiskLimitUtilizationModel(json);
            persistenceProxy.storeRiskLimitUtilization(model, ar -> {
                if (ar.succeeded()) {
                    asyncSecondSnapshotStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
        });
        asyncSecondSnapshotStore.awaitSuccess(30000);
        this.checkCountInCollection(context, MongoPersistenceService.RISK_LIMIT_UTILIZATION_COLLECTION, firstMsgCount);
        this.checkRiskLimitUtilizationCollectionQuery(context);
    }

    private void testErrorInInitialize(TestContext context, String functionToFail, String expectedErrorMessage) throws InterruptedException {
        JsonObject proxyConfig = new JsonObject().put("functionsToFail", new JsonArray().add(functionToFail));
        final PersistenceService persistenceErrorProxy = getPersistenceErrorProxy(proxyConfig);

        persistenceErrorProxy.initialize(context.asyncAssertSuccess());

        // Catch log messages generated by AccountMarginVerticle
        testAppender.start();
        persistenceErrorProxy.initialize(context.asyncAssertSuccess());
        testAppender.waitForMessageContains(Level.ERROR, expectedErrorMessage);
        testAppender.waitForMessageContains(Level.ERROR, "Initialize failed, trying again...");
        testAppender.stop();

        persistenceErrorProxy.close();
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

    private void checkAccountMarginCollectionQuery(TestContext context) {
        JsonObject firstJsonData = DataHelper.getLastJsonFromFile("accountMargin", 1).orElse(new JsonObject());
        JsonObject secondJsonData = DataHelper.getLastJsonFromFile("accountMargin", 2).orElse(new JsonObject());
        AccountMarginModel firstModel = new AccountMarginModel(firstJsonData);
        AccountMarginModel secondModel = new AccountMarginModel(secondJsonData);
        checkCollection(context, firstModel, secondModel, MongoPersistenceService.ACCOUNT_MARGIN_COLLECTION);
    }

    private void checkLiquiGroupMarginCollectionQuery(TestContext context) {
        JsonObject firstJsonData = DataHelper.getLastJsonFromFile("liquiGroupMargin", 1).orElse(new JsonObject());
        JsonObject secondJsonData = DataHelper.getLastJsonFromFile("liquiGroupMargin", 2).orElse(new JsonObject());
        LiquiGroupMarginModel firstModel = new LiquiGroupMarginModel(firstJsonData);
        LiquiGroupMarginModel secondModel = new LiquiGroupMarginModel(secondJsonData);
        checkCollection(context, firstModel, secondModel, MongoPersistenceService.LIQUI_GROUP_MARGIN_COLLECTION);
    }

    private void checkLiquiGroupSplitMarginCollectionQuery(TestContext context) {
        JsonObject firstJsonData = DataHelper.getLastJsonFromFile("liquiGroupSplitMargin", 1).orElse(new JsonObject());
        JsonObject secondJsonData = DataHelper.getLastJsonFromFile("liquiGroupSplitMargin", 2).orElse(new JsonObject());
        LiquiGroupSplitMarginModel firstModel = new LiquiGroupSplitMarginModel(firstJsonData);
        LiquiGroupSplitMarginModel secondModel = new LiquiGroupSplitMarginModel(secondJsonData);
        checkCollection(context, firstModel, secondModel, MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_COLLECTION);
    }

    private void checkPoolMarginCollectionQuery(TestContext context) {
        JsonObject firstJsonData = DataHelper.getLastJsonFromFile("poolMargin", 1).orElse(new JsonObject());
        JsonObject secondJsonData = DataHelper.getLastJsonFromFile("poolMargin", 2).orElse(new JsonObject());
        PoolMarginModel firstModel = new PoolMarginModel(firstJsonData);
        PoolMarginModel secondModel = new PoolMarginModel(secondJsonData);
        checkCollection(context, firstModel, secondModel, MongoPersistenceService.POOL_MARGIN_COLLECTION);
    }

    private void checkPositionReportCollectionQuery(TestContext context) {
        JsonObject firstJsonData = DataHelper.getLastJsonFromFile("positionReport", 1).orElse(new JsonObject());
        JsonObject secondJsonData = DataHelper.getLastJsonFromFile("positionReport", 2).orElse(new JsonObject());
        PositionReportModel firstModel = new PositionReportModel(firstJsonData);
        PositionReportModel secondModel = new PositionReportModel(secondJsonData);
        checkCollection(context, firstModel, secondModel, MongoPersistenceService.POSITION_REPORT_COLLECTION);
    }

    private void checkRiskLimitUtilizationCollectionQuery(TestContext context) {
        JsonObject firstJsonData = DataHelper.getLastJsonFromFile("riskLimitUtilization", 1).orElse(new JsonObject());
        JsonObject secondJsonData = DataHelper.getLastJsonFromFile("riskLimitUtilization", 2).orElse(new JsonObject());
        RiskLimitUtilizationModel firstModel = new RiskLimitUtilizationModel(firstJsonData);
        RiskLimitUtilizationModel secondModel = new RiskLimitUtilizationModel(secondJsonData);
        checkCollection(context, firstModel, secondModel, MongoPersistenceService.RISK_LIMIT_UTILIZATION_COLLECTION);
    }

    private void checkCollection(TestContext context, AbstractModel firstSnapshotModel, AbstractModel secondSnapshotModel, String collectionName) {
        JsonObject param = MongoPersistenceServiceIT.getQueryParams(firstSnapshotModel);
        Assert.assertEquals(param, MongoPersistenceServiceIT.getQueryParams(secondSnapshotModel));
        Async asyncQuery = context.async();
        mongoClient.find(collectionName, param, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(1, ar.result().size());
                JsonObject result = ar.result().get(0);
                JsonArray snapshots = result.getJsonArray("snapshots");
                Assert.assertEquals(2, snapshots.size());
                MongoPersistenceServiceIT.assertSnapshotsContains(context, snapshots, firstSnapshotModel, 0);
                MongoPersistenceServiceIT.assertSnapshotsContains(context, snapshots, secondSnapshotModel, 1);
                asyncQuery.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncQuery.awaitSuccess(5000);
    }

    private PersistenceService getPersistenceErrorProxy(JsonObject config) {
        MongoErrorClient mongoErrorClient = new MongoErrorClient(config);

        ProxyHelper.registerService(PersistenceService.class, vertx, new MongoPersistenceService(vertx, mongoErrorClient), PersistenceService.SERVICE_ADDRESS+"Error");
        return ProxyHelper.createProxy(PersistenceService.class, vertx, PersistenceService.SERVICE_ADDRESS+"Error");
    }

    private static JsonObject getQueryParams(AbstractModel model) {
        JsonObject queryParams = new JsonObject();
        model.getKeys().forEach(key -> queryParams.put(key, model.getValue(key)));
        return queryParams;
    }

    private static void assertSnapshotsContains(TestContext context, JsonArray snapshots, AbstractModel model, int position) {
        JsonObject snapshotData = new JsonObject();
        model.stream()
                .filter(entry -> !model.getKeys().contains(entry.getKey()))
                .forEach(entry -> snapshotData.put(entry.getKey(), entry.getValue()));
        context.assertEquals(snapshotData, snapshots.getJsonObject(position));
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        MongoPersistenceServiceIT.rootLogger.detachAppender(testAppender);
        MongoPersistenceServiceIT.persistenceProxy.close();
        MongoPersistenceServiceIT.vertx.close(context.asyncAssertSuccess());
    }

}
