package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.PersistenceServiceGrpc;
import com.deutscheboerse.risk.dave.StoreReply;
import com.deutscheboerse.risk.dave.config.StoreManagerConfig;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import com.deutscheboerse.risk.dave.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.MessageLite;
import io.grpc.ManagedChannel;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.core.net.TCPSSLOptions;
import io.vertx.grpc.GrpcUniExchange;
import io.vertx.grpc.VertxChannelBuilder;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

public class RestPersistenceService implements PersistenceService {
    private static final Logger LOG = LoggerFactory.getLogger(RestPersistenceService.class);

    private final Vertx vertx;
    private final StoreManagerConfig config;
    private final HealthCheck healthCheck;

    static {
        // Disable grpc info logs
        java.util.logging.Logger grpcLogger = java.util.logging.Logger.getLogger("io.grpc");
        grpcLogger.setLevel(java.util.logging.Level.WARNING);
    }

    @Inject
    public RestPersistenceService(Vertx vertx, @Named("storeManager.conf") JsonObject config) throws IOException {
        this.vertx = vertx;
        this.config = (new ObjectMapper()).readValue(config.toString(), StoreManagerConfig.class);
        this.healthCheck = new HealthCheck(vertx);
    }

    private ManagedChannel createGrpcChannel() {
        return VertxChannelBuilder
                .forAddress(vertx, config.getHostname(), config.getPort())
                .useSsl(this::setGrpcSslOptions)
                .build();
    }

    private void setGrpcSslOptions(TCPSSLOptions sslOptions) {
        PemTrustOptions pemTrustOptions = new PemTrustOptions();
        this.config.getSslTrustCerts()
                .forEach(trustKey -> pemTrustOptions.addCertValue(Buffer.buffer(trustKey)));
        sslOptions
                .setSsl(true)
                .setUseAlpn(true)
                .setPemTrustOptions(pemTrustOptions);
        final String sslCert = this.config.getSslCert();
        final String sslKey = this.config.getSslKey();
        if (sslKey != null && sslCert != null) {
            PemKeyCertOptions pemKeyCertOptions = new PemKeyCertOptions()
                    .setKeyValue(Buffer.buffer(sslKey))
                    .setCertValue(Buffer.buffer(sslCert));
            sslOptions.setPemKeyCertOptions(pemKeyCertOptions);
        }
    }

    @Override
    public void initialize(Handler<AsyncResult<Void>> resultHandler) {
        healthCheck.setComponentReady(HealthCheck.Component.PERSISTENCE_SERVICE);
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void storeAccountMargin(List<AccountMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        ManagedChannel channel = this.createGrpcChannel();
        store(channel, PersistenceServiceGrpc.newVertxStub(channel)::storeAccountMargin, models, resultHandler);
    }

    @Override
    public void storeLiquiGroupMargin(List<LiquiGroupMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        ManagedChannel channel = this.createGrpcChannel();
        store(channel, PersistenceServiceGrpc.newVertxStub(channel)::storeLiquiGroupMargin, models, resultHandler);
    }

    @Override
    public void storeLiquiGroupSplitMargin(List<LiquiGroupSplitMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        ManagedChannel channel = this.createGrpcChannel();
        store(channel, PersistenceServiceGrpc.newVertxStub(channel)::storeLiquiGroupSplitMargin, models, resultHandler);
    }

    @Override
    public void storePoolMargin(List<PoolMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        ManagedChannel channel = this.createGrpcChannel();
        store(channel, PersistenceServiceGrpc.newVertxStub(channel)::storePoolMargin, models, resultHandler);
    }

    @Override
    public void storePositionReport(List<PositionReportModel> models, Handler<AsyncResult<Void>> resultHandler) {
        ManagedChannel channel = this.createGrpcChannel();
        store(channel, PersistenceServiceGrpc.newVertxStub(channel)::storePositionReport, models, resultHandler);
    }

    @Override
    public void storeRiskLimitUtilization(List<RiskLimitUtilizationModel> models, Handler<AsyncResult<Void>> resultHandler) {
        ManagedChannel channel = this.createGrpcChannel();
        store(channel, PersistenceServiceGrpc.newVertxStub(channel)::storeRiskLimitUtilization, models, resultHandler);
    }

    @Override
    public void close() {
        // Empty
    }

    private static <T extends AbstractModel<U>, U extends MessageLite>
    void store(ManagedChannel channel,
               Consumer<Handler<GrpcUniExchange<U, StoreReply>>> storeFunction,
               List<T> models,
               Handler<AsyncResult<Void>> resultHandler) {

        storeFunction.accept(exchange -> {
            exchange
                .handler(ar -> {
                    if (ar.succeeded()) {
                        StoreReply reply = ar.result();
                        if (reply.getSucceeded()) {
                            resultHandler.handle(Future.succeededFuture());
                        } else {
                            LOG.error("Store failed");
                            resultHandler.handle(Future.failedFuture("Store failed"));
                        }
                    } else {
                        LOG.error("Service unavailable", ar);
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                    channel.shutdown();
                });

            models.forEach(model -> exchange.write(model.toGrpc()));

            exchange.end();
        });
    }
}
