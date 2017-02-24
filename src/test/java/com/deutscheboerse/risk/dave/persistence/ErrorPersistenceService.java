package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.model.ModelType;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.serviceproxy.ServiceException;

public class ErrorPersistenceService implements PersistenceService {
    private static final Logger LOG = LoggerFactory.getLogger(ErrorPersistenceService.class);

    private final Vertx vertx;
    private final JsonObject config;

    public ErrorPersistenceService(Vertx vertx) {
        this.vertx = vertx;
        this.config = vertx.getOrCreateContext().config();
    }

    @Override
    public void initialize(Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void store(JsonObject message, ModelType modelType, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }
}
