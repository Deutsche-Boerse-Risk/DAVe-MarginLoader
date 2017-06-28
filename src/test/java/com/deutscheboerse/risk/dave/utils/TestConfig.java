package com.deutscheboerse.risk.dave.utils;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SelfSignedCertificate;

public class TestConfig {

    public static final String BROKER_HOST =  System.getProperty("cil.host", "localhost");
    public static final int BROKER_PORT = Integer.getInteger("cil.tcpport", 5672);
    public static final int STORE_MANAGER_PORT = Integer.getInteger("storage.port", 8443);
    public static final int HEALTHCHECK_PORT = Integer.getInteger("healthCheck.port", 8080);
    public static final SelfSignedCertificate HTTP_SERVER_CERTIFICATE = SelfSignedCertificate.create("localhost");
    public static final SelfSignedCertificate HTTP_CLIENT_CERTIFICATE = SelfSignedCertificate.create("localhost");

    private TestConfig() {
        // Empty
    }

    public static JsonObject getGlobalConfig() {
        return new JsonObject()
                .put("amqp", TestConfig.getAmqpConfig())
                .put("storeManager", TestConfig.getStorageConfig())
                .put("healthCheck", TestConfig.getHealthCheckConfig());
    }

    public static JsonObject getAmqpConfig() {
        return new JsonObject()
                .put("hostname", BROKER_HOST)
                .put("port", BROKER_PORT)
                .put("username", "admin")
                .put("password", "admin")
                .put("reconnectAttempts", -1)
                .put("reconnectTimeout", 5000)
                .put("listeners", new JsonObject()
                        .put("accountMargin", "broadcast.DAVE.PRISMA_DAVEAccountMargin")
                        .put("liquiGroupMargin", "broadcast.DAVE.PRISMA_DAVELiquiGroupMargin")
                        .put("liquiGroupSplitMargin", "broadcast.DAVE.PRISMA_DAVELiquiGroupSplitMargin")
                        .put("poolMargin", "broadcast.DAVE.PRISMA_DAVEPoolMargin")
                        .put("positionReport", "broadcast.DAVE.PRISMA_DAVEPositionReport")
                        .put("riskLimitUtilization", "broadcast.DAVE.PRISMA_DAVERiskLimitUtilization"))
                .put("circuitBreaker", new JsonObject()
                        .put("maxFailures", 1)
                        .put("timeout", 1000)
                        .put("resetTimeout", 2000));
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
                .put("sslKey", pemKeyBuffer.toString())
                .put("sslCert", pemCertBuffer.toString())
                .put("sslTrustCerts", sslTrustCerts);
    }

    public static JsonObject getHealthCheckConfig() {
        return new JsonObject()
                .put("port", HEALTHCHECK_PORT);
    }
}
