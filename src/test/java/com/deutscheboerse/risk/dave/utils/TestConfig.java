package com.deutscheboerse.risk.dave.utils;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SelfSignedCertificate;

public class TestConfig {

    public static final int BROKER_PORT = Integer.getInteger("cil.tcpport", 5672);
    public static final int STORE_MANAGER_PORT = Integer.getInteger("storage.port", 8443);
    public static final int STORE_MANAGER_HEALTHCHECK_PORT = Integer.getInteger("storageHealthCheck.port", 8080);
    public static final int HEALTHCHECK_PORT = Integer.getInteger("healthCheck.port", 8080);
    public static final SelfSignedCertificate HTTP_SERVER_CERTIFICATE = SelfSignedCertificate.create();
    public static final SelfSignedCertificate HTTP_CLIENT_CERTIFICATE = SelfSignedCertificate.create();

    private TestConfig() {
        // Empty
    }

    public static JsonObject getGlobalConfig() {
        return new JsonObject()
                .put("broker", TestConfig.getBrokerConfig())
                .put("storeManager", TestConfig.getStorageConfig())
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

    public static JsonObject getStorageConfig() {
        JsonArray sslTrustCerts = new JsonArray();
        HTTP_SERVER_CERTIFICATE.trustOptions().getCertPaths().forEach(certPath -> {
            Buffer certBuffer = Vertx.vertx().fileSystem().readFileBlocking(certPath);
            sslTrustCerts.add(certBuffer.toString());
        });
        Buffer pemKeyBuffer = Vertx.vertx().fileSystem().readFileBlocking(HTTP_CLIENT_CERTIFICATE.keyCertOptions().getKeyPath());
        Buffer pemCertBuffer = Vertx.vertx().fileSystem().readFileBlocking(HTTP_CLIENT_CERTIFICATE.keyCertOptions().getCertPath());

        return new JsonObject()
                .put("port", STORE_MANAGER_PORT)
                .put("healthCheckPort", STORE_MANAGER_HEALTHCHECK_PORT)
                .put("verifyHost", false)
                .put("sslKey", pemKeyBuffer.toString())
                .put("sslCert", pemCertBuffer.toString())
                .put("sslRequireClientAuth", true)
                .put("sslTrustCerts", sslTrustCerts)
                .put("restApi", new JsonObject()
                        .put("accountMargin", "/api/v1.0/store/am")
                        .put("liquiGroupMargin", "/api/v1.0/store/lgm")
                        .put("liquiGroupSplitMargin", "/api/v1.0/store/lgsm")
                        .put("poolMargin", "/api/v1.0/store/pm")
                        .put("positionReport", "/api/v1.0/store/pr")
                        .put("riskLimitUtilization", "/api/v1.0/store/rlu")
                        .put("healthz", "/healthz"));
    }

    public static JsonObject getHealthCheckConfig() {
        return new JsonObject()
                .put("port", HEALTHCHECK_PORT);
    }
}
