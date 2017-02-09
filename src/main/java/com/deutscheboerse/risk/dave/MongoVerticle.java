package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.model.AccountMarginModel;
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
import io.vertx.ext.mongo.MongoClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class MongoVerticle extends AbstractVerticle {
        final static private Logger LOG = LoggerFactory.getLogger(MongoVerticle.class);

        private static final String DEFAULT_DB_NAME = "DAVe";
        private static final String DEFAULT_CONNECTION_URL = "mongodb://localhost:27017";

        private MongoClient mongo;
        private final List<MessageConsumer<?>> eventBusConsumers = new ArrayList<>();

        @Override
        public void start(Future<Void> fut) throws Exception {
            LOG.info("Starting {} with configuration: {}", MongoVerticle.class.getSimpleName(), config().encodePrettily());

            Future<Void> chainFuture = Future.future();
            connectDb()
                    .compose(this::initDb)
                    .compose(this::startStoreHandlers)
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

        private Future<Void> initDb(Void unused) {
            Future<Void> initDbFuture = Future.future();
            mongo.getCollections(res -> {
                if (res.succeeded()) {
                    List<String> mongoCollections = res.result();
                    List<String> neededCollections = new ArrayList<>(Arrays.asList(
                            AccountMarginModel.MONGO_HISTORY_COLLECTION,
                            AccountMarginModel.MONGO_LATEST_COLLECTION
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

    private Future<Void> startStoreHandlers(Void unused) {
        this.registerConsumer(AccountMarginModel.EB_STORE_ADDRESS, message -> storeAccountMargin(message));

        LOG.info("Event bus store handlers subscribed");
        return Future.succeededFuture();
    }

    private void registerConsumer(String address, Handler<Message<JsonObject>> handler) {
        EventBus eb = vertx.eventBus();
        this.eventBusConsumers.add(eb.consumer(address, handler));
    }

    private void storeAccountMargin(Message<JsonObject> msg) {
        this.store(msg, AccountMarginModel.MONGO_HISTORY_COLLECTION, AccountMarginModel.MONGO_LATEST_COLLECTION);
    }

    private void store(Message<JsonObject> msg, String historyCollection, String latestCollection) {
        List<Future> tasks = new ArrayList<>();
        tasks.add(this.storeIntoHistoryCollection(msg.body(), historyCollection));
        tasks.add(this.storeIntoLatestCollection(msg.body(), latestCollection));
        CompositeFuture.all(tasks).setHandler(ar -> {
            if (ar.succeeded()) {
                msg.reply(new JsonObject());
            } else {
                msg.fail(1, ar.cause().getMessage());
            }
        });
    }

    private Future<String> storeIntoHistoryCollection(JsonObject document, String collection) {
        LOG.trace("Storing message into {} with body {}", collection, document.encodePrettily());
        Future<String> result = Future.future();
        mongo.insert(collection, new JsonObject(document.getMap()), result.completer());
        return result;
    }

    private Future<String> storeIntoLatestCollection(JsonObject document, String collection) {
        LOG.trace("Storing message into {} with body {}", collection, document.encodePrettily());
        Future<String> result = Future.future();
        mongo.insert(collection, new JsonObject(document.getMap()), result.completer());
        return result;
    }

}
