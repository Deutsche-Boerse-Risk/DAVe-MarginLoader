package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import com.deutscheboerse.risk.dave.model.*;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;

import javax.inject.Inject;
import javax.inject.Named;

public class RestPersistenceService implements PersistenceService {
    private static final Logger LOG = LoggerFactory.getLogger(RestPersistenceService.class);

    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final int DEFAULT_PORT = 8443;
    private static final int DEFAULT_HEALTHCHECK_PORT = 8080;
    private static final boolean DEFAULT_VERIFY_HOST = true;

    private static final int RECONNECT_DELAY = 2000;

    private static final String DEFAULT_ACCOUNT_MARGIN_URI = "/api/v1.0/store/am";
    private static final String DEFAULT_LIQUI_GROUP_MARGIN_URI = "/api/v1.0/store/lgm";
    private static final String DEFAULT_LIQUI_SPLIT_MARGIN_URI = "/api/v1.0/store/lgsm";
    private static final String DEFAULT_POSITION_REPORT_URI = "/api/v1.0/store/pr";
    private static final String DEFAULT_POOL_MARGIN_URI = "/api/v1.0/store/pm";
    private static final String DEFAULT_RISK_LIMIT_UTILIZATION_URI = "/api/v1.0/store/rlu";
    private static final String DEFAULT_HEALTHZ_URI = "/healthz";
    private static final Boolean DEFAULT_SSL_REQUIRE_CLIENT_AUTH = false;

    private final Vertx vertx;
    private final JsonObject config;
    private final JsonObject restApi;
    private final HttpClient httpClient;
    private final HealthCheck healthCheck;
    private final ConnectionManager connectionManager = new ConnectionManager();

    @Inject
    public RestPersistenceService(Vertx vertx, @Named("storeManager.conf") JsonObject config) {
        this.vertx = vertx;
        this.config = config;
        this.restApi = this.config.getJsonObject("restApi", new JsonObject());
        this.httpClient = this.createHttpClient();
        this.healthCheck = new HealthCheck(vertx);
    }

    private HttpClient createHttpClient() {
        HttpClientOptions httpClientOptions = this.createHttpClientOptions();
        return this.vertx.createHttpClient(httpClientOptions);
    }

    private HttpClientOptions createHttpClientOptions() {
        HttpClientOptions httpClientOptions = new HttpClientOptions();
        httpClientOptions.setSsl(true);
        httpClientOptions.setVerifyHost(this.config.getBoolean("verifyHost", DEFAULT_VERIFY_HOST));
        PemTrustOptions pemTrustOptions = new PemTrustOptions();
        this.config.getJsonArray("sslTrustCerts", new JsonArray())
                .stream()
                .map(Object::toString)
                .forEach(trustKey -> pemTrustOptions.addCertValue(Buffer.buffer(trustKey)));
        httpClientOptions.setPemTrustOptions(pemTrustOptions);
        if (this.config.getBoolean("sslRequireClientAuth", DEFAULT_SSL_REQUIRE_CLIENT_AUTH)) {
            PemKeyCertOptions pemKeyCertOptions = new PemKeyCertOptions()
                    .setKeyValue(Buffer.buffer(this.config.getString("sslKey")))
                    .setCertValue(Buffer.buffer(this.config.getString("sslCert")));
            httpClientOptions.setPemKeyCertOptions(pemKeyCertOptions);
        }
        return httpClientOptions;
    }

    @Override
    public void initialize(Handler<AsyncResult<Void>> resultHandler) {
        this.connectionManager.ping(ar -> {
            if (ar.succeeded()) {
                healthCheck.setComponentReady(HealthCheck.Component.PERSISTENCE_SERVICE);
            } else {
                // Try to re-initialize in a few seconds
                vertx.setTimer(RECONNECT_DELAY, i -> initialize(res -> {/*empty handler*/}));
                LOG.error("Initialize failed, trying again...");
            }
            // Inform the caller that we succeeded even if the connection to the http server
            // failed. We will try to reconnect automatically on background.
            resultHandler.handle(Future.succeededFuture());
        });
    }

    @Override
    public void storeAccountMargin(AccountMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.postModel(restApi.getString("accountMargin", DEFAULT_ACCOUNT_MARGIN_URI), model, resultHandler);
    }

    @Override
    public void storeLiquiGroupMargin(LiquiGroupMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.postModel(restApi.getString("liquiGroupMargin", DEFAULT_LIQUI_GROUP_MARGIN_URI), model, resultHandler);
    }

    @Override
    public void storeLiquiGroupSplitMargin(LiquiGroupSplitMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.postModel(restApi.getString("liquiGroupSplitMargin", DEFAULT_LIQUI_SPLIT_MARGIN_URI), model, resultHandler);
    }

    @Override
    public void storePoolMargin(PoolMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.postModel(restApi.getString("positionReport", DEFAULT_POSITION_REPORT_URI), model, resultHandler);
    }

    @Override
    public void storePositionReport(PositionReportModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.postModel(restApi.getString("poolMargin", DEFAULT_POOL_MARGIN_URI), model, resultHandler);
    }

    @Override
    public void storeRiskLimitUtilization(RiskLimitUtilizationModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.postModel(restApi.getString("riskLimitUtilization", DEFAULT_RISK_LIMIT_UTILIZATION_URI), model, resultHandler);
    }

    @Override
    public void close() {
        this.httpClient.close();
        this.connectionManager.httpClient.close();
    }

    private void postModel(String requestURI, AbstractModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.httpClient.request(HttpMethod.POST,
                config.getInteger("port", DEFAULT_PORT),
                config.getString("hostname", DEFAULT_HOSTNAME),
                requestURI,
                response -> {
                    if (response.statusCode() == HttpResponseStatus.CREATED.code()) {
                        response.bodyHandler(body -> resultHandler.handle(Future.succeededFuture()));
                    } else {
                        LOG.error("{} failed: {}", requestURI, response.statusMessage());
                        connectionManager.startReconnection();
                        resultHandler.handle(Future.failedFuture(response.statusMessage()));
                    }
                }).exceptionHandler(e -> {
                    LOG.error("{} failed: {}", requestURI, e.getMessage());
                    connectionManager.startReconnection();
                    resultHandler.handle(Future.failedFuture(e.getMessage()));
                }).putHeader("content-type", "application/json").end(model.toString());
    }

    private class ConnectionManager {

        private HttpClient httpClient = vertx.createHttpClient();

        void startReconnection() {
            if (healthCheck.isComponentReady(HealthCheck.Component.PERSISTENCE_SERVICE)) {
                // Inform other components that we have failed
                healthCheck.setComponentFailed(HealthCheck.Component.PERSISTENCE_SERVICE);
                // Re-check the connection
                scheduleConnectionStatus();
            }
        }

        void ping(Handler<AsyncResult<Void>> resultHandler) {
            httpClient.get(
                    config.getInteger("healthCheckPort", DEFAULT_HEALTHCHECK_PORT),
                    config.getString("hostname", DEFAULT_HOSTNAME),
                    restApi.getString("healthz", DEFAULT_HEALTHZ_URI),
                    response -> {
                        if (response.statusCode() == HttpResponseStatus.OK.code()) {
                            resultHandler.handle(Future.succeededFuture());
                        } else {
                            resultHandler.handle(Future.failedFuture(response.statusMessage()));
                        }
                    }).exceptionHandler(e ->
                        resultHandler.handle(Future.failedFuture(e.getMessage()))
                    ).end();
        }

        private void scheduleConnectionStatus() {
            vertx.setTimer(RECONNECT_DELAY, id -> checkConnectionStatus());
        }

        private void checkConnectionStatus() {
            this.ping(res -> {
                if (res.succeeded()) {
                    LOG.info("Back online");
                    healthCheck.setComponentReady(HealthCheck.Component.PERSISTENCE_SERVICE);
                } else {
                    LOG.error("Still disconnected");
                    scheduleConnectionStatus();
                }
            });
        }
    }
}
