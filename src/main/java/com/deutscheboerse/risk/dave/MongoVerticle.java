package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.model.AbstractModel;
import com.deutscheboerse.risk.dave.model.AccountMarginModel;
import com.deutscheboerse.risk.dave.model.LiquiGroupMarginModel;
import com.deutscheboerse.risk.dave.model.PoolMarginModel;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.IndexOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.MongoClientUpdateResult;
import io.vertx.ext.mongo.UpdateOptions;

import java.util.*;


public class MongoVerticle extends AbstractVerticle {
        private static final Logger LOG = LoggerFactory.getLogger(MongoVerticle.class);

        private static final String DEFAULT_DB_NAME = "DAVe";
        private static final String DEFAULT_CONNECTION_URL = "mongodb://localhost:27017/?waitqueuemultiple=20000";

        private MongoClient mongo;
        private final List<MessageConsumer<?>> eventBusConsumers = new ArrayList<>();

        @Override
        public void start(Future<Void> fut) throws Exception {
            LOG.info("Starting {} with configuration: {}", MongoVerticle.class.getSimpleName(), config().encodePrettily());

            Future<Void> chainFuture = Future.future();
            connectDb()
                    .compose(i -> initDb())
                    .compose(i -> createIndexes())
                    .compose(i -> startStoreHandlers())
                    .compose(chainFuture::complete, chainFuture);
            chainFuture.setHandler(ar -> {
                if (ar.succeeded()) {
                    LOG.info("MongoDB verticle started");
                    fut.complete();
                } else {
                    LOG.error("MongoDB verticle failed to deploy", chainFuture.cause());
                    fut.fail(chainFuture.cause());
                }
            });
        }

        private Future<Void> connectDb() {
            JsonObject config = new JsonObject();
            config.put("db_name", config().getString("dbName", MongoVerticle.DEFAULT_DB_NAME));
            config.put("useObjectId", true);
            config.put("connection_string", config().getString("connectionUrl", MongoVerticle.DEFAULT_CONNECTION_URL));
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

    private Future<Void> createIndexes() {
        Future<Void> createIndexesFuture = Future.future();

        List<AbstractModel> models = new ArrayList<>();
        models.add(new AccountMarginModel());
        models.add(new LiquiGroupMarginModel());
        models.add(new PoolMarginModel());

        List<Future> futs = new ArrayList<>();
        models.forEach(model -> {
            IndexOptions indexOptions = new IndexOptions().name("unique_idx").unique(true);

            Future<Void> historyIndexFuture = Future.future();
            mongo.createIndexWithOptions(model.getHistoryCollection(), model.getHistoryUniqueIndex(), indexOptions, historyIndexFuture.completer());
            futs.add(historyIndexFuture);

            Future<Void> latestIndexFuture = Future.future();
            mongo.createIndexWithOptions(model.getLatestCollection(), model.getLatestUniqueIndex(), indexOptions, latestIndexFuture.completer());
            futs.add(latestIndexFuture);
        });

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

    private Future<Void> startStoreHandlers() {
        this.registerConsumer(AccountMarginModel.EB_STORE_ADDRESS, message -> store(message, new AccountMarginModel()));
        this.registerConsumer(LiquiGroupMarginModel.EB_STORE_ADDRESS, message -> store(message, new LiquiGroupMarginModel()));
        this.registerConsumer(PoolMarginModel.EB_STORE_ADDRESS, message -> store(message, new PoolMarginModel()));

        LOG.info("Event bus store handlers subscribed");
        return Future.succeededFuture();
    }

    private void registerConsumer(String address, Handler<Message<JsonObject>> handler) {
        EventBus eb = vertx.eventBus();
        this.eventBusConsumers.add(eb.consumer(address, handler));
    }

    private void store(Message<JsonObject> msg, AbstractModel model) {
        List<Future> tasks = new ArrayList<>();
        model.mergeIn(msg.body());
        tasks.add(this.storeIntoHistoryCollection(model));
        tasks.add(this.storeIntoLatestCollection(model));
        CompositeFuture.all(tasks).setHandler(ar -> {
            if (ar.succeeded()) {
                msg.reply(new JsonObject());
            } else {
                msg.fail(1, ar.cause().getMessage());
            }
        });
    }

    private Future<String> storeIntoHistoryCollection(AbstractModel model) {
        JsonObject document = model.copy();
        LOG.trace("Storing message into {} with body {}", model.getHistoryCollection(), document.encodePrettily());
        Future<String> result = Future.future();
        mongo.insert(model.getHistoryCollection(), document, result.completer());
        return result;
    }

    private Future<MongoClientUpdateResult> storeIntoLatestCollection(AbstractModel model) {
        JsonObject document = model.copy();
        LOG.trace("Storing message into {} with body {}", model.getLatestCollection(), document.encodePrettily());
        Future<MongoClientUpdateResult> result = Future.future();
        mongo.replaceDocumentsWithOptions(model.getLatestCollection(),
                model.getLatestQueryParams(),
                document,
                new UpdateOptions().setUpsert(true),
                result.completer());
        return result;

    }

}
