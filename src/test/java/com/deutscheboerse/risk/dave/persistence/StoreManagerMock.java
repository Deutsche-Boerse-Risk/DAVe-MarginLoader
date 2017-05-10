package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.*;
import com.deutscheboerse.risk.dave.utils.TestConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.grpc.GrpcReadStream;
import io.vertx.grpc.VertxServer;
import io.vertx.grpc.VertxServerBuilder;

public class StoreManagerMock {
    private static final Logger LOG = LoggerFactory.getLogger(StoreManagerMock.class);

    private final Vertx vertx;
    private final VertxServer server;
    private boolean health = true;

    private PersistenceServiceGrpc.PersistenceServiceVertxImplBase service = new PersistenceServiceGrpc.PersistenceServiceVertxImplBase() {
        @Override
        public void storeAccountMargin(GrpcReadStream<AccountMargin> request, Future<StoreReply> response) {
            LOG.trace("Received storeAccountMargin request");
            setRequestHandler(request, response);
        }

        @Override
        public void storeLiquiGroupMargin(GrpcReadStream<LiquiGroupMargin> request, Future<StoreReply> response) {
            LOG.trace("Received storeLiquiGroupMargin request");
            setRequestHandler(request, response);
        }

        @Override
        public void storeLiquiGroupSplitMargin(GrpcReadStream<LiquiGroupSplitMargin> request, Future<StoreReply> response) {
            LOG.trace("Received storeLiquiGroupSplitMargin request");
            setRequestHandler(request, response);
        }

        @Override
        public void storePoolMargin(GrpcReadStream<PoolMargin> request, Future<StoreReply> response) {
            LOG.trace("Received storePoolMargin request");
            setRequestHandler(request, response);
        }

        @Override
        public void storePositionReport(GrpcReadStream<PositionReport> request, Future<StoreReply> response) {
            LOG.trace("Received storePositionReport request");
            setRequestHandler(request, response);
        }

        @Override
        public void storeRiskLimitUtilization(GrpcReadStream<RiskLimitUtilization> request, Future<StoreReply> response) {
            LOG.trace("Received storeRiskLimitUtilization request");
            setRequestHandler(request, response);
        }

        private <T> void setRequestHandler(GrpcReadStream<T> request, Future<StoreReply> response) {
            request.handler(model -> {
                LOG.trace(model.toString());
            }).endHandler(v -> {
                LOG.trace("Request has ended.");
                response.complete(StoreReply.newBuilder().setSucceeded(health).build());
            });
        }
    };

    StoreManagerMock(Vertx vertx) {
        this.vertx = vertx;
        this.server = this.createGrpcServer();
    }

    StoreManagerMock listen(Handler<AsyncResult<Void>> resultHandler) {
        LOG.info("Starting web server on port {}", TestConfig.STORE_MANAGER_PORT);

        this.server.start(resultHandler);
        return this;
    }

    StoreManagerMock setHealth(boolean health) {
        this.health = health;
        return this;
    }

    private VertxServer createGrpcServer() {
        return VertxServerBuilder
                .forPort(vertx, TestConfig.STORE_MANAGER_PORT)
                .addService(service)
                .useSsl(options -> options
                        .setSsl(true)
                        .setUseAlpn(true)
                        .setPemKeyCertOptions(TestConfig.HTTP_SERVER_CERTIFICATE.keyCertOptions())
                        .setPemTrustOptions(TestConfig.HTTP_CLIENT_CERTIFICATE.trustOptions())
                )
                .build();
    }

    public void close(Handler<AsyncResult<Void>> completionHandler) {
        LOG.info("Shutting down webserver");
        server.shutdown(completionHandler);
    }
}
