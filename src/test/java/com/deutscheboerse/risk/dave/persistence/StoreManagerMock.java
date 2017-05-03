package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.utils.TestConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class StoreManagerMock {
    private static final Logger LOG = LoggerFactory.getLogger(StoreManagerMock.class);

    private static final String ACCOUNT_MARGIN_URI = "/api/v1.0/store/am";
    private static final String LIQUI_GROUP_MARGIN_URI = "/api/v1.0/store/lgm";
    private static final String LIQUI_GROUP_SPLIT_MARGIN_URI = "/api/v1.0/store/lgsm";
    private static final String POSITION_REPORT_URI = "/api/v1.0/store/pr";
    private static final String POOL_MARGIN_URI = "/api/v1.0/store/pm";
    private static final String RISK_LIMIT_UTILIZATION_URI = "/api/v1.0/store/rlu";

    private final Vertx vertx;
    private final HttpServer server;
    private boolean health = true;

    StoreManagerMock(Vertx vertx) {
        this.vertx = vertx;
        this.server = this.createHttpServer();
    }

    StoreManagerMock listen(Handler<AsyncResult<Void>> resultHandler) {

        int storeManagerPort = TestConfig.STORE_MANAGER_PORT;
        LOG.info("Starting web server on port {}", storeManagerPort);

        Future<HttpServer> listenFuture = Future.future();
        server.listen(storeManagerPort, listenFuture);
        listenFuture.map((Void)null).setHandler(resultHandler);
        return this;
    }

    StoreManagerMock setHealth(boolean health) {
        this.health = health;
        return this;
    }

    private HttpServer createHttpServer() {
        Router router = configureRouter();

        HttpServerOptions httpServerOptions = this.createHttpServerOptions();
        return vertx.createHttpServer(httpServerOptions).requestHandler(router::accept);
    }

    private HttpServerOptions createHttpServerOptions() {
        HttpServerOptions httpOptions = new HttpServerOptions();
        this.setSSL(httpOptions);
        return httpOptions;
    }

    private void setSSL(HttpServerOptions httpServerOptions) {
        httpServerOptions.setSsl(true);
        httpServerOptions.setPemKeyCertOptions(TestConfig.HTTP_SERVER_CERTIFICATE.keyCertOptions());
        httpServerOptions.setPemTrustOptions(TestConfig.HTTP_CLIENT_CERTIFICATE.trustOptions());
    }

    private Router configureRouter() {
        Router router = Router.router(vertx);

        LOG.info("Adding route REST API");
        router.post(ACCOUNT_MARGIN_URI).handler(this::storeAccountMargin);
        router.post(LIQUI_GROUP_MARGIN_URI).handler(this::storeLiquiGroupMargin);
        router.post(LIQUI_GROUP_SPLIT_MARGIN_URI).handler(this::storeLiquiGroupSplitMargin);
        router.post(POOL_MARGIN_URI).handler(this::storePoolMargin);
        router.post(POSITION_REPORT_URI).handler(this::storePositionReport);
        router.post(RISK_LIMIT_UTILIZATION_URI).handler(this::storeRiskLimitUtilization);

        return router;
    }


    private void storeAccountMargin(RoutingContext routingContext) {
        LOG.trace("Received storeAccountMargin request");
        routingContext.response().setStatusCode(health ? 201: 503).end();
    }

    private void storeLiquiGroupMargin(RoutingContext routingContext) {
        LOG.trace("Received storeLiquiGroupMargin request");
        routingContext.response().setStatusCode(health ? 201: 503).end();
    }

    private void storeLiquiGroupSplitMargin(RoutingContext routingContext) {
        LOG.trace("Received storeLiquiGroupSplitMargin request");
        routingContext.response().setStatusCode(health ? 201: 503).end();
    }

    private void storePoolMargin(RoutingContext routingContext) {
        LOG.trace("Received storePoolMargin request");
        routingContext.response().setStatusCode(health ? 201: 503).end();
    }

    private void storePositionReport(RoutingContext routingContext) {
        LOG.trace("Received storePositionReport request");
        routingContext.response().setStatusCode(health ? 201: 503).end();
    }

    private void storeRiskLimitUtilization(RoutingContext routingContext) {
        LOG.trace("Received storeRiskLimitUtilization request");
        routingContext.response().setStatusCode(health ? 201: 503).end();
    }

    public void close(Handler<AsyncResult<Void>> completionHandler) {
        LOG.info("Shutting down webserver");
        server.close(completionHandler);
    }
}
