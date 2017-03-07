package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.BaseTest;
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
    private static Vertx vertx;
    private static MongoClient mongoClient;
    private static PersistenceService persistenceProxy;

    @BeforeClass
    public static void setUp(TestContext context) {
        MongoPersistenceServiceIT.vertx = Vertx.vertx();
        JsonObject config = BaseTest.getMongoConfig();
        JsonObject mongoConfig = BaseTest.getMongoClientConfig(config);

        MongoPersistenceServiceIT.mongoClient = MongoClient.createShared(MongoPersistenceServiceIT.vertx, mongoConfig);

        ProxyHelper.registerService(PersistenceService.class, vertx, new MongoPersistenceService(vertx), PersistenceService.SERVICE_ADDRESS);
        MongoPersistenceServiceIT.persistenceProxy = ProxyHelper.createProxy(PersistenceService.class, vertx, PersistenceService.SERVICE_ADDRESS);
        MongoPersistenceServiceIT.persistenceProxy.initialize(config, context.asyncAssertSuccess());
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

        Async asyncStore2 = context.async(1704);
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

        this.checkCountInCollection(context, MongoPersistenceService.ACCOUNT_MARGIN_HISTORY_COLLECTION, 3408);
        this.checkCountInCollection(context, MongoPersistenceService.ACCOUNT_MARGIN_LATEST_COLLECTION, 1704);
        this.checkAccountMarginHistoryCollectionQuery(context);
        this.checkAccountMarginLatestCollectionQuery(context);
    }

    @Test
    public void testLiquiGroupMarginStore(TestContext context) throws IOException {
        Async asyncStore1 = context.async(2171);
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

        Async asyncStore2 = context.async(2171);
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

        this.checkCountInCollection(context, MongoPersistenceService.LIQUI_GROUP_MARGIN_HISTORY_COLLECTION, 4342);
        this.checkCountInCollection(context, MongoPersistenceService.LIQUI_GROUP_MARGIN_LATEST_COLLECTION, 2171);
        this.checkLiquiGroupMarginHistoryCollectionQuery(context);
        this.checkLiquiGroupMarginLatestCollectionQuery(context);
    }

    @Test
    public void testLiquiGroupSplitMarginStore(TestContext context) throws IOException {
        Async asyncStore1 = context.async(2472);
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

        Async asyncStore2 = context.async(2472);
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

        this.checkCountInCollection(context, MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_HISTORY_COLLECTION, 4944);
        this.checkCountInCollection(context, MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_LATEST_COLLECTION, 2472);
        this.checkLiquiGroupSplitMarginHistoryCollectionQuery(context);
        this.checkLiquiGroupSplitMarginLatestCollectionQuery(context);
    }

    @Test
    public void testPoolMarginStore(TestContext context) throws IOException {
        Async asyncFirstSnapshotStore = context.async(270);
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
        Async asyncSecondSnapshotStore = context.async(270);
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
        this.checkCountInCollection(context, MongoPersistenceService.POOL_MARGIN_HISTORY_COLLECTION, 540);
        this.checkCountInCollection(context, MongoPersistenceService.POOL_MARGIN_LATEST_COLLECTION, 270);
        this.checkPoolMarginHistoryCollectionQuery(context);
        this.checkPoolMarginLatestCollectionQuery(context);
    }

    @Test
    public void testPositionReportStore(TestContext context) throws IOException {
        Async asyncFirstSnapshotStore = context.async(3596);
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
        Async asyncSecondSnapshotStore = context.async(3596);
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
        this.checkCountInCollection(context, MongoPersistenceService.POSITION_REPORT_HISTORY_COLLECTION, 7192);
        this.checkCountInCollection(context, MongoPersistenceService.POSITION_REPORT_LATEST_COLLECTION, 3596);
        this.checkPositionReportHistoryCollectionQuery(context);
        this.checkPositionReportLatestCollectionQuery(context);
    }

    @Test
    public void testRiskLimitUtilizationStore(TestContext context) throws IOException {
        Async asyncFirstSnapshotStore = context.async(2);
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
        Async asyncSecondSnapshotStore = context.async(2);
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
        this.checkCountInCollection(context, MongoPersistenceService.RISK_LIMIT_UTILIZATION_HISTORY_COLLECTION, 4);
        this.checkCountInCollection(context, MongoPersistenceService.RISK_LIMIT_UTILIZATION_LATEST_COLLECTION, 2);
        this.checkRiskLimitUtilizationHistoryCollectionQuery(context);
        this.checkRiskLimitUtilizationLatestCollectionQuery(context);
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
        JsonObject jsonData = DataHelper.getLastJsonFromFile("accountMargin", 1).get();
        AccountMarginModel model = new AccountMarginModel(jsonData);
        JsonObject param = MongoPersistenceServiceIT.getQueryParams(model);
        Async asyncQuery = context.async();
        FindOptions findOptions = new FindOptions()
                .setSort(new JsonObject().put("snapshotID", 1));
        MongoPersistenceServiceIT.mongoClient.findWithOptions(MongoPersistenceService.ACCOUNT_MARGIN_HISTORY_COLLECTION, param, findOptions, ar -> {
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

    private void checkAccountMarginLatestCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("accountMargin", 2).get();
        AccountMarginModel model = new AccountMarginModel(jsonData);
        JsonObject param = MongoPersistenceServiceIT.getQueryParams(model);
        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.find(MongoPersistenceService.ACCOUNT_MARGIN_LATEST_COLLECTION, param, ar -> {
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

    private void checkLiquiGroupMarginHistoryCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("liquiGroupMargin", 1).get();
        LiquiGroupMarginModel model = new LiquiGroupMarginModel(jsonData);
        JsonObject param = MongoPersistenceServiceIT.getQueryParams(model);
        Async asyncQuery = context.async();
        FindOptions findOptions = new FindOptions()
                .setSort(new JsonObject().put("snapshotID", 1));
        MongoPersistenceServiceIT.mongoClient.findWithOptions(MongoPersistenceService.LIQUI_GROUP_MARGIN_HISTORY_COLLECTION, param, findOptions, ar -> {
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

    private void checkLiquiGroupMarginLatestCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("liquiGroupMargin", 2).get();
        LiquiGroupMarginModel model = new LiquiGroupMarginModel(jsonData);
        JsonObject param = MongoPersistenceServiceIT.getQueryParams(model);
        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.find(MongoPersistenceService.LIQUI_GROUP_MARGIN_LATEST_COLLECTION, param, ar -> {
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

    private void checkLiquiGroupSplitMarginHistoryCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("liquiGroupSplitMargin", 1).get();
        LiquiGroupSplitMarginModel model = new LiquiGroupSplitMarginModel(jsonData);
        JsonObject param = MongoPersistenceServiceIT.getQueryParams(model);
        FindOptions findOptions = new FindOptions()
                .setSort(new JsonObject().put("snapshotID", 1));

        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.findWithOptions(MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_HISTORY_COLLECTION, param, findOptions, ar -> {
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

    private void checkLiquiGroupSplitMarginLatestCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("liquiGroupSplitMargin", 2).get();
        LiquiGroupSplitMarginModel model = new LiquiGroupSplitMarginModel(jsonData);
        JsonObject param = MongoPersistenceServiceIT.getQueryParams(model);
        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.find(MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_LATEST_COLLECTION, param, ar -> {
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

    private void checkPoolMarginHistoryCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("poolMargin", 1).get();
        PoolMarginModel model = new PoolMarginModel(jsonData);
        JsonObject param = MongoPersistenceServiceIT.getQueryParams(model);
        FindOptions findOptions = new FindOptions()
                .setSort(new JsonObject().put("snapshotID", 1));

        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.findWithOptions(MongoPersistenceService.POOL_MARGIN_HISTORY_COLLECTION, param, findOptions, ar -> {
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

    private void checkPoolMarginLatestCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("poolMargin", 2).get();
        PoolMarginModel model = new PoolMarginModel(jsonData);
        JsonObject param = MongoPersistenceServiceIT.getQueryParams(model);
        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.find(MongoPersistenceService.POOL_MARGIN_LATEST_COLLECTION, param, ar -> {
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

    private void checkPositionReportHistoryCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("positionReport", 1).get();
        PositionReportModel model = new PositionReportModel(jsonData);
        JsonObject param = MongoPersistenceServiceIT.getQueryParams(model);
        FindOptions findOptions = new FindOptions()
                .setSort(new JsonObject().put("snapshotID", 1));

        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.findWithOptions(MongoPersistenceService.POSITION_REPORT_HISTORY_COLLECTION, param, findOptions, ar -> {
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

    private void checkPositionReportLatestCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("positionReport", 2).get();
        PositionReportModel model = new PositionReportModel(jsonData);
        JsonObject param = MongoPersistenceServiceIT.getQueryParams(model);
        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.find(MongoPersistenceService.POSITION_REPORT_LATEST_COLLECTION, param, ar -> {
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

    private void checkRiskLimitUtilizationHistoryCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("riskLimitUtilization", 1).get();
        RiskLimitUtilizationModel model = new RiskLimitUtilizationModel(jsonData);
        JsonObject param = MongoPersistenceServiceIT.getQueryParams(model);
        FindOptions findOptions = new FindOptions()
                .setSort(new JsonObject().put("snapshotID", 1));
        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.findWithOptions(MongoPersistenceService.RISK_LIMIT_UTILIZATION_HISTORY_COLLECTION, param, findOptions, ar -> {
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

    private void checkRiskLimitUtilizationLatestCollectionQuery(TestContext context) {
        JsonObject jsonData = DataHelper.getLastJsonFromFile("riskLimitUtilization", 2).get();
        RiskLimitUtilizationModel model = new RiskLimitUtilizationModel(jsonData);
        JsonObject param = MongoPersistenceServiceIT.getQueryParams(model);
        Async asyncQuery = context.async();
        MongoPersistenceServiceIT.mongoClient.find(MongoPersistenceService.RISK_LIMIT_UTILIZATION_LATEST_COLLECTION, param, ar -> {
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

    private static JsonObject getExpectedMongoResultFromModel(JsonObject model) {
        JsonObject result = new JsonObject();
        model.fieldNames().stream()
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
        model.getKeys().stream().forEach(key -> queryParams.put(key, model.getValue(key)));
        return queryParams;
    }

    private static void assertModelEqualsResult(TestContext context, JsonObject model, JsonObject mongoResult) {
        mongoResult.remove("_id");
        context.assertEquals(MongoPersistenceServiceIT.getExpectedMongoResultFromModel(model), mongoResult);
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        MongoPersistenceServiceIT.persistenceProxy.close();
        MongoPersistenceServiceIT.vertx.close(context.asyncAssertSuccess());
    }

}
