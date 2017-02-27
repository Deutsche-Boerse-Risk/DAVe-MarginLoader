package com.deutscheboerse.risk.dave.persistence;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

public class MongoPersistenceService implements PersistenceService {
    private static final Logger LOG = LoggerFactory.getLogger(MongoPersistenceService.class);

    private static final String DEFAULT_DB_NAME = "DAVe";
    private static final String DEFAULT_CONNECTION_URL = "mongodb://localhost:27017/?waitqueuemultiple=20000";

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

    private final Vertx vertx;
    private MongoClient mongo;

    @Inject
    public MongoPersistenceService(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public void initialize(JsonObject config, Handler<AsyncResult<Void>> resultHandler) {
        connectDb(config)
                .compose(i -> initDb())
                .compose(i -> createIndexes())
                .setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(ServiceException.fail(INIT_ERROR, ar.cause().getMessage()));
            }
        });
    }

    @Override
    public void storeAccountMargin(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        AbstractModel model = new AccountMarginModel();
        model.mergeIn(message);
        this.store(model, ACCOUNT_MARGIN_HISTORY_COLLECTION, ACCOUNT_MARGIN_LATEST_COLLECTION, resultHandler);
    }

    @Override
    public void storeLiquiGroupMargin(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        AbstractModel model = new LiquiGroupMarginModel();
        model.mergeIn(message);
        this.store(model, LIQUI_GROUP_MARGIN_HISTORY_COLLECTION, LIQUI_GROUP_MARGIN_LATEST_COLLECTION, resultHandler);
    }

    @Override
    public void storeLiquiGroupSplitMargin(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        AbstractModel model = new LiquiGroupSplitMarginModel();
        model.mergeIn(message);
        this.store(model, LIQUI_GROUP_SPLIT_MARGIN_HISTORY_COLLECTION, LIQUI_GROUP_SPLIT_MARGIN_LATEST_COLLECTION, resultHandler);
    }

    @Override
    public void storePoolMargin(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        AbstractModel model = new PoolMarginModel();
        model.mergeIn(message);
        this.store(model, POOL_MARGIN_HISTORY_COLLECTION, POOL_MARGIN_LATEST_COLLECTION, resultHandler);
    }

    @Override
    public void storePositionReport(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        AbstractModel model = new PositionReportModel();
        model.mergeIn(message);
        this.store(model, POSITION_REPORT_HISTORY_COLLECTION, POSITION_REPORT_LATEST_COLLECTION, resultHandler);
    }

    @Override
    public void storeRiskLimitUtilization(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {
        AbstractModel model = new RiskLimitUtilizationModel();
        model.mergeIn(message);
        this.store(model, RISK_LIMIT_UTILIZATION_HISTORY_COLLECTION, RISK_LIMIT_UTILIZATION_LATEST_COLLECTION, resultHandler);
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
                resultHandler.handle(ServiceException.fail(STORE_ERROR, ar.cause().getMessage()));
            }
        });
    }

    public static JsonObject getLatestQueryParams(AbstractModel model) {
        JsonObject queryParams = new JsonObject();
        model.getKeys().stream().forEach(key -> queryParams.put(key, model.getValue(key)));
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

    private Future<String> storeIntoHistoryCollection(AbstractModel model, String collection) {
        JsonObject document = new JsonObject().mergeIn(model);
        LOG.trace("Storing message into {} with body {}", collection, document.encodePrettily());
        Future<String> result = Future.future();
        mongo.insert(collection, document, result.completer());
        return result;
    }

    private Future<MongoClientUpdateResult> storeIntoLatestCollection(AbstractModel model, String collection, JsonObject queryParams) {
        JsonObject document = new JsonObject().mergeIn(model);
        LOG.trace("Storing message into {} with body {}", collection, document.encodePrettily());
        Future<MongoClientUpdateResult> result = Future.future();
        mongo.replaceDocumentsWithOptions(collection,
                queryParams,
                document,
                new UpdateOptions().setUpsert(true),
                result.completer());
        return result;

    }

    private Future<Void> connectDb(JsonObject config) {
        JsonObject mongoConfig = new JsonObject();
        mongoConfig.put("db_name", config.getString("dbName", MongoPersistenceService.DEFAULT_DB_NAME));
        mongoConfig.put("useObjectId", true);
        mongoConfig.put("connection_string", config.getString("connectionUrl", MongoPersistenceService.DEFAULT_CONNECTION_URL));
        mongo = MongoClient.createShared(vertx, mongoConfig);
        LOG.info("Connected to MongoDB");
        return Future.succeededFuture();
    }

    private Future<Void> initDb() {
        Future<Void> initDbFuture = Future.future();
        mongo.getCollections(res -> {
            if (res.succeeded()) {
                List<String> mongoCollections = res.result();
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

                List<Future> futs = new ArrayList<>();

                neededCollections.stream()
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
        BiConsumer<String, JsonObject> indexCheck = (collectionName, index) -> {
            IndexOptions indexOptions = new IndexOptions().name("unique_idx").unique(true);
            Future<Void> historyIndexFuture = Future.future();
            mongo.createIndexWithOptions(collectionName, index, indexOptions, historyIndexFuture.completer());
            futs.add(historyIndexFuture);
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
