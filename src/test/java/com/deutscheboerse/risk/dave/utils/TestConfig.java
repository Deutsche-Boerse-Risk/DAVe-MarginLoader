package com.deutscheboerse.risk.dave.utils;

import io.vertx.core.json.JsonObject;

public class TestConfig {

    private static final int BROKER_PORT = Integer.getInteger("cil.tcpport", 5672);
    private static final int PERSISTENCE_PORT = Integer.getInteger("persistence.port", 8084);
    public static final int HTTP_PORT = Integer.getInteger("http.port", 8083);

    private TestConfig() {
        // Empty
    }

    public static JsonObject getGlobalConfig() {
        return new JsonObject()
                .put("broker", TestConfig.getBrokerConfig())
                .put("persistence", TestConfig.getPersistenceConfig())
                .put("healthCheck", TestConfig.getHealthCheckConfig());
    }

    public static JsonObject getBrokerConfig() {
        return new JsonObject()
                .put("port", BROKER_PORT)
                .put("username", "admin")
                .put("password", "admin")
                .put("reconnectAttempts", -1)
                .put("reconnectTimeout", 5000)
                .put("listeners", new JsonObject()
                        .put("accountMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEAccountMargin")
                        .put("liquiGroupMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupMargin")
                        .put("liquiGroupSplitMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupSplitMargin")
                        .put("poolMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEPoolMargin")
                        .put("positionReport", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEPositionReport")
                        .put("riskLimitUtilization", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVERiskLimitUtilization"));
    }

    private static JsonObject getPersistenceConfig() {
        return new JsonObject()
                .put("port", PERSISTENCE_PORT)
                .put("restApi", new JsonObject()
                        .put("accountMargin", "/accountMargin/store")
                        .put("liquiGroupMargin", "/liquiGroupMargin/store")
                        .put("liquiGroupSplitMargin", "/liquiGroupSplitMargin/store")
                        .put("positionReport", "/positionReport/store")
                        .put("poolMargin", "/poolMargin/store")
                        .put("riskLimitUtilization", "/riskLimitUtilization/store")
                        .put("healthz", "/healthz"));
    }

    public static JsonObject getHealthCheckConfig() {
        return new JsonObject()
                .put("port", HTTP_PORT);
    }
}
