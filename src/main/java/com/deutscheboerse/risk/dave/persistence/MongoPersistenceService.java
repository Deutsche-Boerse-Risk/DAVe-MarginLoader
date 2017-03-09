package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck.Component;
import com.deutscheboerse.risk.dave.model.*;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.IndexOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.MongoClientUpdateResult;
import io.vertx.ext.mongo.UpdateOptions;
import io.vertx.serviceproxy.ServiceException;

import javax.inject.Inject;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.BiConsumer;

public class MongoPersistenceService implements PersistenceService {
    private static final Logger LOG = LoggerFactory.getLogger(MongoPersistenceService.class);

    public static final String ACCOUNT_MARGIN_HISTORY_COLLECTION = "AccountMargin";
    public static final String ACCOUNT_MARGIN_LATEST_COLLECTION = "AccountMargin.latest";
    public static final String LIQUI_GROUP_MARGIN_HISTORY_COLLECTION = "LiquiGroupMargin";
    public static final String LIQUI_GROUP_MARGIN_LATEST_COLLECTION = "LiquiGroupMargin.latest";
    public static final String LIQUI_GROUP_SPLIT_MARGIN_HISTORY_COLLECTION = "LiquiGroupSplitMargin";
    public static final String LIQUI_GROUP_SPLIT_MARGIN_LATEST_COLLECTION = "LiquiGroupSplitMargin.latest";
    public static final String POOL_MARGIN_HISTORY_COLLECTION = "PoolMargin";
    public static final String POOL_MARGIN_LATEST_COLLECTION = "PoolMargin.latest";
    public static final String POSITION_REPORT_HISTORY_COLLECTION = "PositionReport";
    public static final String POSITION_REPORT_LATEST_COLLECTION = "PositionReport.latest";
    public static final String RISK_LIMIT_UTILIZATION_HISTORY_COLLECTION = "RiskLimitUtilization";
    public static final String RISK_LIMIT_UTILIZATION_LATEST_COLLECTION = "RiskLimitUtilization.latest";

    private static final int RECONNECT_DELAY = 2000;

    private final Vertx vertx;
    private final MongoClient mongo;
    private final HealthCheck healthCheck;

    @Inject
    public MongoPersistenceService(Vertx vertx, MongoClient mongo) {
        this.vertx = vertx;
        this.healthCheck = new HealthCheck(this.vertx);
        this.mongo = mongo;
    }

    @Override
    public void initialize(Handler<AsyncResult<Void>> resultHandler) {
        initDb()
                .compose(i -> createIndexes())
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        healthCheck.setComponentReady(Component.PERSISTENCE_SERVICE);
                    } else {
                        // Try to re-initialize in a few seconds
                        vertx.setTimer(RECONNECT_DELAY, i -> initialize(res -> {/*empty handler*/}));
                        LOG.error("Initialize failed, trying again...");
                    }
                    // Inform the caller that we succeeded even if the connection to mongo database
                    // failed. We will try to reconnect automatically on background.
                    resultHandler.handle(Future.succeededFuture());
                });
    }

    @Override
    public void storeAccountMargin(AccountMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, ACCOUNT_MARGIN_HISTORY_COLLECTION, ACCOUNT_MARGIN_LATEST_COLLECTION, resultHandler);
    }

    @Override
    public void storeLiquiGroupMargin(LiquiGroupMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, LIQUI_GROUP_MARGIN_HISTORY_COLLECTION, LIQUI_GROUP_MARGIN_LATEST_COLLECTION, resultHandler);
    }

    @Override
    public void storeLiquiGroupSplitMargin(LiquiGroupSplitMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, LIQUI_GROUP_SPLIT_MARGIN_HISTORY_COLLECTION, LIQUI_GROUP_SPLIT_MARGIN_LATEST_COLLECTION, resultHandler);
    }

    @Override
    public void storePoolMargin(PoolMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, POOL_MARGIN_HISTORY_COLLECTION, POOL_MARGIN_LATEST_COLLECTION, resultHandler);
    }

    @Override
    public void storePositionReport(PositionReportModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, POSITION_REPORT_HISTORY_COLLECTION, POSITION_REPORT_LATEST_COLLECTION, resultHandler);
    }

    @Override
    public void storeRiskLimitUtilization(RiskLimitUtilizationModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, RISK_LIMIT_UTILIZATION_HISTORY_COLLECTION, RISK_LIMIT_UTILIZATION_LATEST_COLLECTION, resultHandler);
    }

    @Override
    public void close() {
        this.mongo.close();
    }

    private void store(AbstractModel model, String historyCollection, String latestCollection, Handler<AsyncResult<Void>> resultHandler) {
        JsonObject latestQueryParams = MongoPersistenceService.getLatestQueryParams(model);
        List<Future> tasks = new ArrayList<>();
        tasks.add(this.storeIntoHistoryCollection(model, historyCollection));
        tasks.add(this.storeIntoLatestCollection(model, latestCollection, latestQueryParams));
        CompositeFuture.all(tasks).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                if (healthCheck.isComponentReady(Component.PERSISTENCE_SERVICE)) {
                    // Inform other components that we have failed
                    healthCheck.setComponentFailed(Component.PERSISTENCE_SERVICE);
                    // Re-check the connection
                    scheduleConnectionStatus();
                }
                resultHandler.handle(ServiceException.fail(STORE_ERROR, ar.cause().getMessage()));
            }
        });
    }

    private void scheduleConnectionStatus() {
        vertx.setTimer(RECONNECT_DELAY, id -> checkConnectionStatus());
    }

    private void checkConnectionStatus() {
        this.mongo.runCommand("dbstats", new JsonObject().put("dbstats", 1), res -> {
            if (res.succeeded()) {
                LOG.info("Back online");
                healthCheck.setComponentReady(Component.PERSISTENCE_SERVICE);
            } else {
                LOG.error("Still disconnected");
                scheduleConnectionStatus();
            }
        });
    }

    public static JsonObject getLatestQueryParams(AbstractModel model) {
        JsonObject queryParams = new JsonObject();
        model.getKeys().forEach(key -> queryParams.put(key, model.getValue(key)));
        return queryParams;
    }

    public static JsonObject getHistoryUniqueIndex(AbstractModel model) {
        JsonObject uniqueIndex = new JsonObject();
        uniqueIndex.put("snapshotID", 1);
        uniqueIndex.mergeIn(MongoPersistenceService.getLatestUniqueIndex(model));
        return uniqueIndex;
    }

    public static JsonObject getLatestUniqueIndex(AbstractModel model) {
        JsonObject uniqueIndex = new JsonObject();
        model.getKeys().stream().forEach(key -> uniqueIndex.put(key, 1));
        return uniqueIndex;
    }

    public static JsonObject getStoreDocument(AbstractModel model) {
        JsonObject document = new JsonObject().mergeIn(model);
        Instant instant = Instant.ofEpochMilli(document.getLong("timestamp"));
        document.put("timestamp", new JsonObject().put("$date", ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)));
        return document;
    }

    public static Collection<String> getRequiredCollections() {
        List<String> neededCollections = new ArrayList<>(Arrays.asList(
                ACCOUNT_MARGIN_HISTORY_COLLECTION,
                ACCOUNT_MARGIN_LATEST_COLLECTION,
                LIQUI_GROUP_MARGIN_HISTORY_COLLECTION,
                LIQUI_GROUP_MARGIN_LATEST_COLLECTION,
                LIQUI_GROUP_SPLIT_MARGIN_HISTORY_COLLECTION,
                LIQUI_GROUP_SPLIT_MARGIN_LATEST_COLLECTION,
                POOL_MARGIN_HISTORY_COLLECTION,
                POOL_MARGIN_LATEST_COLLECTION,
                POSITION_REPORT_HISTORY_COLLECTION,
                POSITION_REPORT_LATEST_COLLECTION,
                RISK_LIMIT_UTILIZATION_HISTORY_COLLECTION,
                RISK_LIMIT_UTILIZATION_LATEST_COLLECTION
        ));
        return Collections.unmodifiableList(neededCollections);
    }

    private Future<String> storeIntoHistoryCollection(AbstractModel model, String collection) {
        JsonObject document = MongoPersistenceService.getStoreDocument(model);
        LOG.trace("Storing message into {} with body {}", collection, document.encodePrettily());
        Future<String> result = Future.future();
        mongo.insert(collection, document, result.completer());
        return result;
    }

    private Future<MongoClientUpdateResult> storeIntoLatestCollection(AbstractModel model, String collection, JsonObject queryParams) {
        JsonObject document = MongoPersistenceService.getStoreDocument(model);
        LOG.trace("Storing message into {} with body {}", collection, document.encodePrettily());
        Future<MongoClientUpdateResult> result = Future.future();
        mongo.replaceDocumentsWithOptions(collection,
                queryParams,
                document,
                new UpdateOptions().setUpsert(true),
                result.completer());
        return result;

    }

    private Future<Void> initDb() {
        Future<Void> initDbFuture = Future.future();
        mongo.getCollections(res -> {
            if (res.succeeded()) {
                List<String> mongoCollections = res.result();
                List<Future> futs = new ArrayList<>();
                MongoPersistenceService.getRequiredCollections().stream()
                        .filter(collection -> ! mongoCollections.contains(collection))
                        .forEach(collection -> {
                            LOG.info("Collection {} is missing and will be added", collection);
                            Future<Void> fut = Future.future();
                            mongo.createCollection(collection, fut.completer());
                            futs.add(fut);
                        });
                CompositeFuture.all(futs).setHandler(ar -> {
                    if (ar.succeeded()) {
                        LOG.info("Mongo has all needed collections for DAVe");
                        LOG.info("Initialized MongoDB");
                        initDbFuture.complete();
                    } else {
                        LOG.error("Failed to add all collections needed for DAVe to Mongo", ar.cause());
                        initDbFuture.fail(ar.cause());
                    }
                });
            } else {
                LOG.error("Failed to get collection list", res.cause());
                initDbFuture.fail(res.cause());
            }
        });
        return initDbFuture;
    }

    private Future<Void> createIndexes() {
        Future<Void> createIndexesFuture = Future.future();

        List<Future> futs = new ArrayList<>();
        BiConsumer<String, JsonObject> indexCreate = (collectionName, index) -> {
            IndexOptions indexOptions = new IndexOptions().name("unique_idx").unique(true);
            Future<Void> indexFuture = Future.future();
            mongo.createIndexWithOptions(collectionName, index, indexOptions, indexFuture.completer());
            futs.add(indexFuture);
        };

        indexCreate.accept(MongoPersistenceService.ACCOUNT_MARGIN_HISTORY_COLLECTION, MongoPersistenceService.getHistoryUniqueIndex(new AccountMarginModel()));
        indexCreate.accept(MongoPersistenceService.ACCOUNT_MARGIN_LATEST_COLLECTION, MongoPersistenceService.getLatestUniqueIndex(new AccountMarginModel()));
        indexCreate.accept(MongoPersistenceService.LIQUI_GROUP_MARGIN_HISTORY_COLLECTION, MongoPersistenceService.getHistoryUniqueIndex(new LiquiGroupMarginModel()));
        indexCreate.accept(MongoPersistenceService.LIQUI_GROUP_MARGIN_LATEST_COLLECTION, MongoPersistenceService.getLatestUniqueIndex(new LiquiGroupMarginModel()));
        indexCreate.accept(MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_HISTORY_COLLECTION, MongoPersistenceService.getHistoryUniqueIndex(new LiquiGroupSplitMarginModel()));
        indexCreate.accept(MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_LATEST_COLLECTION, MongoPersistenceService.getLatestUniqueIndex(new LiquiGroupSplitMarginModel()));
        indexCreate.accept(MongoPersistenceService.POOL_MARGIN_HISTORY_COLLECTION, MongoPersistenceService.getHistoryUniqueIndex(new PoolMarginModel()));
        indexCreate.accept(MongoPersistenceService.POOL_MARGIN_LATEST_COLLECTION, MongoPersistenceService.getLatestUniqueIndex(new PoolMarginModel()));
        indexCreate.accept(MongoPersistenceService.POSITION_REPORT_HISTORY_COLLECTION, MongoPersistenceService.getHistoryUniqueIndex(new PositionReportModel()));
        indexCreate.accept(MongoPersistenceService.POSITION_REPORT_LATEST_COLLECTION, MongoPersistenceService.getLatestUniqueIndex(new PositionReportModel()));
        indexCreate.accept(MongoPersistenceService.RISK_LIMIT_UTILIZATION_HISTORY_COLLECTION, MongoPersistenceService.getHistoryUniqueIndex(new RiskLimitUtilizationModel()));
        indexCreate.accept(MongoPersistenceService.RISK_LIMIT_UTILIZATION_LATEST_COLLECTION, MongoPersistenceService.getLatestUniqueIndex(new RiskLimitUtilizationModel()));

        CompositeFuture.all(futs).setHandler(ar -> {
            if (ar.succeeded()) {
                LOG.info("Mongo has all needed indexes");
                createIndexesFuture.complete();
            } else {
                LOG.error("Failed to create all needed indexes in Mongo", ar.cause());
                createIndexesFuture.fail(ar.cause());
            }
        });
        return createIndexesFuture;
    }
}
