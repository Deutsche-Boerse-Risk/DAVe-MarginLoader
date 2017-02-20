package com.deutscheboerse.risk.dave.model;

import io.vertx.core.json.JsonObject;

public interface MongoModel {
    String getHistoryCollection();
    String getLatestCollection();
    JsonObject getLatestQueryParams();
    JsonObject getHistoryUniqueIndex();
    JsonObject getLatestUniqueIndex();
}
