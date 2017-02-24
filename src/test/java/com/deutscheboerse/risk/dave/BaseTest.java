package com.deutscheboerse.risk.dave;

import io.vertx.core.json.JsonObject;

import java.util.UUID;

public class BaseTest {
    protected static final int BROKER_PORT = Integer.getInteger("cil.tcpport", 5672);
    protected static final int DB_PORT =  Integer.getInteger("mongodb.port", 27017);
    protected static final int HTTP_PORT = Integer.getInteger("http.port", 8083);


    protected static JsonObject getGlobalConfig() {
        JsonObject globalConfig = new JsonObject()
                .put("broker", BaseTest.getBrokerConfig())
                .put("mongo", BaseTest.getMongoConfig())
                .put("healthCheck", BaseTest.getHealtCheckConfig());
        return globalConfig;
    }

    protected static JsonObject getBrokerConfig() {
        JsonObject brokerConfig = new JsonObject()
                .put("port", BROKER_PORT)
                .put("listeners", new JsonObject()
                        .put("accountMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEAccountMargin")
                        .put("liquiGroupMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupMargin")
                        .put("liquiGroupSplitMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupSplitMargin")
                        .put("poolMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEPoolMargin")
                        .put("positionReport", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEPositionReport")
                        .put("riskLimitUtilization", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVERiskLimitUtilization"));
        return brokerConfig;
    }

    protected static JsonObject getMongoConfig() {
        final String DB_NAME = "DAVe-Test" + UUID.randomUUID().getLeastSignificantBits();
        JsonObject mongoConfig = new JsonObject()
                .put("dbName", DB_NAME)
                .put("connectionUrl", String.format("mongodb://localhost:%s/?waitqueuemultiple=%d", DB_PORT, 1000));
        return mongoConfig;
    }

    protected static JsonObject getHealtCheckConfig() {
        JsonObject healthCheckConfig = new JsonObject()
                .put("port", HTTP_PORT);
        return healthCheckConfig;
    }

    protected static JsonObject getMongoClientConfig(JsonObject mongoVerticleConfig) {
        JsonObject mongoConfig = new JsonObject()
                .put("db_name", mongoVerticleConfig.getString("dbName"))
                .put("connection_string", mongoVerticleConfig.getString("connectionUrl"));
        return mongoConfig;
    }
}
