package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.config.StoreManagerConfig;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import com.deutscheboerse.risk.dave.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;

public class RestPersistenceService implements PersistenceService {
    private static final Logger LOG = LoggerFactory.getLogger(RestPersistenceService.class);

    private static final String ACCOUNT_MARGIN_URI = "/api/v1.0/store/am";
    private static final String LIQUI_GROUP_MARGIN_URI = "/api/v1.0/store/lgm";
    private static final String LIQUI_GROUP_SPLIT_MARGIN_URI = "/api/v1.0/store/lgsm";
    private static final String POSITION_REPORT_URI = "/api/v1.0/store/pr";
    private static final String POOL_MARGIN_URI = "/api/v1.0/store/pm";
    private static final String RISK_LIMIT_UTILIZATION_URI = "/api/v1.0/store/rlu";

    private final Vertx vertx;
    private final StoreManagerConfig config;
    private final HttpClient httpClient;
    private final HealthCheck healthCheck;

    @Inject
    public RestPersistenceService(Vertx vertx, @Named("storeManager.conf") JsonObject config) throws IOException {
        this.vertx = vertx;
        this.config = (new ObjectMapper()).readValue(config.toString(), StoreManagerConfig.class);
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
        httpClientOptions.setVerifyHost(this.config.isVerifyHost());
        PemTrustOptions pemTrustOptions = new PemTrustOptions();
        this.config.getSslTrustCerts()
                .forEach(trustKey -> pemTrustOptions.addCertValue(Buffer.buffer(trustKey)));
        httpClientOptions.setPemTrustOptions(pemTrustOptions);
        final String sslKey = this.config.getSslKey();
        final String sslCert = this.config.getSslCert();
        if (sslKey != null && sslCert != null) {
            PemKeyCertOptions pemKeyCertOptions = new PemKeyCertOptions()
                    .setKeyValue(Buffer.buffer(sslKey))
                    .setCertValue(Buffer.buffer(sslCert));
            httpClientOptions.setPemKeyCertOptions(pemKeyCertOptions);
        }
        return httpClientOptions;
    }

    @Override
    public void initialize(Handler<AsyncResult<Void>> resultHandler) {
        healthCheck.setComponentReady(HealthCheck.Component.PERSISTENCE_SERVICE);
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void storeAccountMargin(AccountMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.postModel(ACCOUNT_MARGIN_URI, model, resultHandler);
    }

    @Override
    public void storeLiquiGroupMargin(LiquiGroupMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.postModel(LIQUI_GROUP_MARGIN_URI, model, resultHandler);
    }

    @Override
    public void storeLiquiGroupSplitMargin(LiquiGroupSplitMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.postModel(LIQUI_GROUP_SPLIT_MARGIN_URI, model, resultHandler);
    }

    @Override
    public void storePoolMargin(PoolMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.postModel(POOL_MARGIN_URI, model, resultHandler);
    }

    @Override
    public void storePositionReport(PositionReportModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.postModel(POSITION_REPORT_URI, model, resultHandler);
    }

    @Override
    public void storeRiskLimitUtilization(RiskLimitUtilizationModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.postModel(RISK_LIMIT_UTILIZATION_URI, model, resultHandler);
    }

    @Override
    public void close() {
        this.httpClient.close();
    }

    private void postModel(String requestURI, AbstractModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.httpClient.request(HttpMethod.POST,
                config.getPort(),
                config.getHostname(),
                requestURI,
                response -> {
                    if (response.statusCode() == HttpResponseStatus.CREATED.code()) {
                        response.bodyHandler(body -> resultHandler.handle(Future.succeededFuture()));
                    } else {
                        LOG.error("{} failed: {}", requestURI, response.statusMessage());
                        resultHandler.handle(Future.failedFuture(response.statusMessage()));
                    }
                }).exceptionHandler(e -> {
                    LOG.error("{} failed: {}", requestURI, e.getMessage());
                    resultHandler.handle(Future.failedFuture(e.getMessage()));
                }).putHeader("content-type", "application/json").end(model.toString());
    }
}
