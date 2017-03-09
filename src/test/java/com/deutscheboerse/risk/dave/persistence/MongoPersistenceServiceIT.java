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
        JsonObject proxyConfig = new JsonObject().put("functionsToFail", new JsonArray().add("insert"));

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
        JsonObject proxyConfig = new JsonObject().put("functionsToFail", new JsonArray().add("insert").add("runCommand"));

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

        this.checkCountInCollection(context, MongoPersistenceService.ACCOUNT_MARGIN_HISTORY_COLLECTION, firstMsgCount + secondMsgCount);
        this.checkCountInCollection(context, MongoPersistenceService.ACCOUNT_MARGIN_LATEST_COLLECTION, firstMsgCount);
        this.checkAccountMarginHistoryCollectionQuery(context);
        this.checkAccountMarginLatestCollectionQuery(context);
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

        this.checkCountInCollection(context, MongoPersistenceService.LIQUI_GROUP_MARGIN_HISTORY_COLLECTION, firstMsgCount + secondMsgCount);
        this.checkCountInCollection(context, MongoPersistenceService.LIQUI_GROUP_MARGIN_LATEST_COLLECTION, firstMsgCount);
        this.checkLiquiGroupMarginHistoryCollectionQuery(context);
        this.checkLiquiGroupMarginLatestCollectionQuery(context);
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

        this.checkCountInCollection(context, MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_HISTORY_COLLECTION, firstMsgCount + secondMsgCount);
        this.checkCountInCollection(context, MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_LATEST_COLLECTION, firstMsgCount);
        this.checkLiquiGroupSplitMarginHistoryCollectionQuery(context);
        this.checkLiquiGroupSplitMarginLatestCollectionQuery(context);
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
        this.checkCountInCollection(context, MongoPersistenceService.POOL_MARGIN_HISTORY_COLLECTION, firstMsgCount + secondMsgCount);
        this.checkCountInCollection(context, MongoPersistenceService.POOL_MARGIN_LATEST_COLLECTION, firstMsgCount);
        this.checkPoolMarginHistoryCollectionQuery(context);
        this.checkPoolMarginLatestCollectionQuery(context);
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
        this.checkCountInCollection(context, MongoPersistenceService.POSITION_REPORT_HISTORY_COLLECTION, firstMsgCount + secondMsgCount);
        this.checkCountInCollection(context, MongoPersistenceService.POSITION_REPORT_LATEST_COLLECTION, firstMsgCount);
        this.checkPositionReportHistoryCollectionQuery(context);
        this.checkPositionReportLatestCollectionQuery(context);
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
        this.checkCountInCollection(context, MongoPersistenceService.RISK_LIMIT_UTILIZATION_HISTORY_COLLECTION, firstMsgCount + secondMsgCount);
        this.checkCountInCollection(context, MongoPersistenceService.RISK_LIMIT_UTILIZATION_LATEST_COLLECTION, firstMsgCount);
        this.checkRiskLimitUtilizationHistoryCollectionQuery(context);
        this.checkRiskLimitUtilizationLatestCollectionQuery(context);
    }

    private void testErrorInInitialize(TestContext context, String functionToFail, String expectedErrorMessage) throws InterruptedException {
        JsonObject proxyConfig = new JsonObject().put("functionsToFail", new JsonArray().add(functionToFail));
        final PersistenceService persistenceErrorProxy = getPersistenceErrorProxy(proxyConfig);

        persistenceErrorProxy.initialize(context.asyncAssertSuccess());

        // Catch log messages generated by AccountMarginVerticle
        testAppender.start();
        persistenceErrorProxy.initialize(context.asyncAssertSuccess());
        testAppender.waitForMessageContains(Level.ERROR, expectedErrorMessage);
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

    private void checkAccountMarginHistoryCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("accountMargin", 1).orElse(new JsonObject());
        AccountMarginModel model = new AccountMarginModel(jsonData);
        checkHistoryCollection(context, model, MongoPersistenceService.ACCOUNT_MARGIN_HISTORY_COLLECTION);
    }

    private void checkAccountMarginLatestCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("accountMargin", 2).orElse(new JsonObject());
        AccountMarginModel model = new AccountMarginModel(jsonData);
        checkLatestCollection(context, model, MongoPersistenceService.ACCOUNT_MARGIN_LATEST_COLLECTION);
    }

    private void checkLiquiGroupMarginHistoryCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("liquiGroupMargin", 1).orElse(new JsonObject());
        LiquiGroupMarginModel model = new LiquiGroupMarginModel(jsonData);
        checkHistoryCollection(context, model, MongoPersistenceService.LIQUI_GROUP_MARGIN_HISTORY_COLLECTION);
    }

    private void checkLiquiGroupMarginLatestCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("liquiGroupMargin", 2).orElse(new JsonObject());
        LiquiGroupMarginModel model = new LiquiGroupMarginModel(jsonData);
        checkLatestCollection(context, model, MongoPersistenceService.LIQUI_GROUP_MARGIN_LATEST_COLLECTION);
    }

    private void checkLiquiGroupSplitMarginHistoryCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("liquiGroupSplitMargin", 1).orElse(new JsonObject());
        LiquiGroupSplitMarginModel model = new LiquiGroupSplitMarginModel(jsonData);
        checkHistoryCollection(context, model, MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_HISTORY_COLLECTION);
    }

    private void checkLiquiGroupSplitMarginLatestCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("liquiGroupSplitMargin", 2).orElse(new JsonObject());
        LiquiGroupSplitMarginModel model = new LiquiGroupSplitMarginModel(jsonData);
        checkLatestCollection(context, model, MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_LATEST_COLLECTION);
    }

    private void checkPoolMarginHistoryCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("poolMargin", 1).orElse(new JsonObject());
        PoolMarginModel model = new PoolMarginModel(jsonData);
        checkHistoryCollection(context, model, MongoPersistenceService.POOL_MARGIN_HISTORY_COLLECTION);
    }

    private void checkPoolMarginLatestCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("poolMargin", 2).orElse(new JsonObject());
        PoolMarginModel model = new PoolMarginModel(jsonData);
        checkLatestCollection(context, model, MongoPersistenceService.POOL_MARGIN_LATEST_COLLECTION);
    }

    private void checkPositionReportHistoryCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("positionReport", 1).orElse(new JsonObject());
        PositionReportModel model = new PositionReportModel(jsonData);
        checkHistoryCollection(context, model, MongoPersistenceService.POSITION_REPORT_HISTORY_COLLECTION);
    }

    private void checkPositionReportLatestCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("positionReport", 2).orElse(new JsonObject());
        PositionReportModel model = new PositionReportModel(jsonData);
        checkLatestCollection(context, model, MongoPersistenceService.POSITION_REPORT_LATEST_COLLECTION);
    }

    private void checkRiskLimitUtilizationHistoryCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("riskLimitUtilization", 1).orElse(new JsonObject());
        RiskLimitUtilizationModel model = new RiskLimitUtilizationModel(jsonData);
        checkHistoryCollection(context, model, MongoPersistenceService.RISK_LIMIT_UTILIZATION_HISTORY_COLLECTION);
    }

    private void checkRiskLimitUtilizationLatestCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("riskLimitUtilization", 2).orElse(new JsonObject());
        RiskLimitUtilizationModel model = new RiskLimitUtilizationModel(jsonData);
        checkLatestCollection(context, model, MongoPersistenceService.RISK_LIMIT_UTILIZATION_LATEST_COLLECTION);
    }

    private void checkHistoryCollection(TestContext context, AbstractModel model, String collectionName) {
        JsonObject param = MongoPersistenceServiceIT.getQueryParams(model);
        FindOptions findOptions = new FindOptions()
                .setSort(new JsonObject().put("snapshotID", 1));
        Async asyncQuery = context.async();
        mongoClient.findWithOptions(collectionName, param, findOptions, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(2, ar.result().size());
                MongoPersistenceServiceIT.assertModelEqualsResult(context, model, ar.result().get(0));
                asyncQuery.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncQuery.awaitSuccess(5000);
    }

    private void checkLatestCollection(TestContext context, AbstractModel model, String collectionName) {
        JsonObject param = MongoPersistenceServiceIT.getQueryParams(model);
        Async asyncQuery = context.async();
        mongoClient.find(collectionName, param, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(1, ar.result().size());
                MongoPersistenceServiceIT.assertModelEqualsResult(context, model, ar.result().get(0));
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

    private static JsonObject getExpectedMongoResultFromModel(JsonObject model) {
        JsonObject result = new JsonObject();
        model.fieldNames()
                .forEach(key -> {
                    if (key.equals("timestamp")) {
                        Instant instant = Instant.ofEpochMilli(model.getLong("timestamp"));
                        result.put(key, new JsonObject().put("$date", ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)));
                    } else {
                        result.put(key, model.getValue(key));
                    }
                });
        return result;
    }

    private static JsonObject getQueryParams(AbstractModel model) {
        JsonObject queryParams = new JsonObject();
        model.getKeys().forEach(key -> queryParams.put(key, model.getValue(key)));
        return queryParams;
    }

    private static void assertModelEqualsResult(TestContext context, JsonObject model, JsonObject mongoResult) {
        mongoResult.remove("_id");
        context.assertEquals(MongoPersistenceServiceIT.getExpectedMongoResultFromModel(model), mongoResult);
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        MongoPersistenceServiceIT.rootLogger.detachAppender(testAppender);
        MongoPersistenceServiceIT.persistenceProxy.close();
        MongoPersistenceServiceIT.vertx.close(context.asyncAssertSuccess());
    }

}
