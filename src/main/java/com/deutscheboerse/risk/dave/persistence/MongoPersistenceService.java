package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.model.*;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.MongoClientUpdateResult;
import io.vertx.ext.mongo.UpdateOptions;
import io.vertx.serviceproxy.ServiceException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MongoPersistenceService implements PersistenceService {
    private static final Logger LOG = LoggerFactory.getLogger(MongoPersistenceService.class);

    private static final String DEFAULT_DB_NAME = "DAVe";
    private static final String DEFAULT_CONNECTION_URL = "mongodb://localhost:27017/?waitqueuemultiple=20000";

    private final Vertx vertx;
    private final JsonObject config;
    private MongoClient mongo;

    public MongoPersistenceService(Vertx vertx) {
        this.vertx = vertx;
        this.config = vertx.getOrCreateContext().config();
    }

    @Override
    public void initialize(Handler<AsyncResult<Void>> resultHandler) {
        connectDb()
                .compose(i -> initDb())
                .setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(ServiceException.fail(INIT_ERROR, ar.cause().getMessage()));
            }
        });

    }

    @Override
    public void store(JsonObject message, ModelType modelType, Handler<AsyncResult<Void>> resultHandler) {
        AbstractModel model;
        switch(modelType) {
            case ACCOUNT_MARGIN_MODEL:
                model = new AccountMarginModel();
                break;
            case LIQUI_GROUP_MARGIN_MODEL:
                model = new LiquiGroupMarginModel();
                break;
            case POOL_MARGIN_MODEL:
                model = new PoolMarginModel();
                break;
            default:
                resultHandler.handle(ServiceException.fail(STORE_UNKNOWN_MODEL_ERROR, "Unknown model"));
                return;
        }
        model.mergeIn(message);
        List<Future> tasks = new ArrayList<>();
        tasks.add(this.storeIntoHistoryCollection(model));
        tasks.add(this.storeIntoLatestCollection(model));
        CompositeFuture.all(tasks).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(ServiceException.fail(STORE_ERROR, ar.cause().getMessage()));
            }
        });
    }

    private Future<String> storeIntoHistoryCollection(AbstractModel model) {
        JsonObject document = new JsonObject().mergeIn(model);
        LOG.trace("Storing message into {} with body {}", model.getHistoryCollection(), document.encodePrettily());
        Future<String> result = Future.future();
        mongo.insert(model.getHistoryCollection(), document, result.completer());
        return result;
    }

    private Future<MongoClientUpdateResult> storeIntoLatestCollection(AbstractModel model) {
        JsonObject document = new JsonObject().mergeIn(model);
        LOG.trace("Storing message into {} with body {}", model.getLatestCollection(), document.encodePrettily());
        Future<MongoClientUpdateResult> result = Future.future();
        mongo.replaceDocumentsWithOptions(model.getLatestCollection(),
                model.getLatestQueryParams(),
                document,
                new UpdateOptions().setUpsert(true),
                result.completer());
        return result;

    }

    private Future<Void> connectDb() {
        JsonObject config = new JsonObject();
        config.put("db_name", config.getString("dbName", MongoPersistenceService.DEFAULT_DB_NAME));
        config.put("useObjectId", true);
        config.put("connection_string", config.getString("connectionUrl", MongoPersistenceService.DEFAULT_CONNECTION_URL));
        mongo = MongoClient.createShared(vertx, config);
        LOG.info("Connected to MongoDB");
        return Future.succeededFuture();
    }

    private Future<Void> initDb() {
        Future<Void> initDbFuture = Future.future();
        mongo.getCollections(res -> {
            if (res.succeeded()) {
                List<String> mongoCollections = res.result();
                List<String> neededCollections = new ArrayList<>(Arrays.asList(
                        AccountMarginModel.MONGO_HISTORY_COLLECTION,
                        AccountMarginModel.MONGO_LATEST_COLLECTION,
                        LiquiGroupMarginModel.MONGO_HISTORY_COLLECTION,
                        LiquiGroupMarginModel.MONGO_LATEST_COLLECTION,
                        PoolMarginModel.MONGO_HISTORY_COLLECTION,
                        PoolMarginModel.MONGO_LATEST_COLLECTION
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

}
