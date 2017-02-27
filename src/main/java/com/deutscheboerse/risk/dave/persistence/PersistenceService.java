package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.model.ModelType;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

@ProxyGen
public interface PersistenceService {
    String SERVICE_ADDRESS = "persistenceService";

    int INIT_ERROR = 2;
    int STORE_UNKNOWN_MODEL_ERROR = 3;
    int STORE_ERROR = 4;

    void initialize(JsonObject config, Handler<AsyncResult<Void>> resultHandler);

    void store(JsonObject message, ModelType modelType, Handler<AsyncResult<Void>> resultHandler);
}
