package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.model.ModelType;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ProxyHelper;

@ProxyGen
public interface PersistenceService {
    int INIT_ERROR = 2;
    int STORE_UNKNOWN_MODEL_ERROR = 3;
    int STORE_ERROR = 4;

    static PersistenceService create(Vertx vertx) {
        return new MongoPersistenceService(vertx);
    }

    static PersistenceService createProxy(Vertx vertx, String address) {
        return ProxyHelper.createProxy(PersistenceService.class, vertx, address);
    }

    void initialize(Handler<AsyncResult<Void>> resultHandler);

    void store(JsonObject message, ModelType modelType, Handler<AsyncResult<Void>> resultHandler);
}
