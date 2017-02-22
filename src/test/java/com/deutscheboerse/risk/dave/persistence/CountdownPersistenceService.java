package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.model.ModelType;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class CountdownPersistenceService implements PersistenceService {
    private static final Logger LOG = LoggerFactory.getLogger(CountdownPersistenceService.class);

    private final Vertx vertx;
    private final JsonObject config;
    private Async async;
    private JsonObject lastMessage;

    public CountdownPersistenceService(Vertx vertx, Async async) {
        this.vertx = vertx;
        this.config = vertx.getOrCreateContext().config();
        this.async = async;
    }

    @Override
    public void initialize(Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void store(JsonObject message, ModelType modelType, Handler<AsyncResult<Void>> resultHandler) {

        // Store the message
        this.lastMessage = message;

        this.async.countDown();

        // Always succeeds
        resultHandler.handle(Future.succeededFuture());
    }

    public JsonObject getLastMessage() {
        return this.lastMessage;
    }
}
