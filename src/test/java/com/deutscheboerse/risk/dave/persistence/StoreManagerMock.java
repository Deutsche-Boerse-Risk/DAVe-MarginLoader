package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.utils.TestConfig;
import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class StoreManagerMock {
    private static final Logger LOG = LoggerFactory.getLogger(StoreManagerMock.class);

    private final Vertx vertx;
    private final HttpServer server;
    private final HttpServer healthCheckServer;
    private boolean health = true;

    StoreManagerMock(Vertx vertx) {
        this.vertx = vertx;
        this.server = this.createHttpServer();
        this.healthCheckServer = this.createHealthCheckServer();
    }

    StoreManagerMock listen(Handler<AsyncResult<Void>> resultHandler) {

        int storeManagerPort = TestConfig.STORE_MANAGER_PORT;
        int healthCheckPort = TestConfig.STORE_MANAGER_HEALTHCHECK_PORT;
        LOG.info("Starting web server on port {} with health check port {}", storeManagerPort, healthCheckPort);

        Future<HttpServer> listenFuture = Future.future();
        Future<HttpServer> healthCheckListenFuture = Future.future();
        server.listen(storeManagerPort, listenFuture);
        healthCheckServer.listen(healthCheckPort, healthCheckListenFuture);

        CompositeFuture.all(listenFuture, healthCheckListenFuture).map((Void) null).setHandler(resultHandler);
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

    private HttpServer createHealthCheckServer() {
        HealthCheckHandler healthCheckHandler = HealthCheckHandler.create(vertx);
        healthCheckHandler.register("healthz", this::healthz);
        Router router = Router.router(vertx);
        router.get("/healthz").handler(healthCheckHandler);

        return vertx.createHttpServer().requestHandler(router::accept);
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

        JsonObject restApi = TestConfig.getStorageConfig().getJsonObject("restApi", new JsonObject());

        LOG.info("Adding route REST API");
        router.post(restApi.getString("accountMargin")).handler(this::storeAccountMargin);
        router.post(restApi.getString("liquiGroupMargin")).handler(this::storeLiquiGroupMargin);
        router.post(restApi.getString("liquiGroupSplitMargin")).handler(this::storeLiquiGroupSplitMargin);
        router.post(restApi.getString("poolMargin")).handler(this::storePoolMargin);
        router.post(restApi.getString("positionReport")).handler(this::storePositionReport);
        router.post(restApi.getString("riskLimitUtilization")).handler(this::storeRiskLimitUtilization);

        return router;
    }

    private void healthz(Future<Status> future) {
        future.complete(this.health ? Status.OK() : Status.KO());
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
        Future<Void> serverClose = Future.future();
        Future<Void> healthCheckClose = Future.future();
        server.close(serverClose);
        healthCheckServer.close(healthCheckClose);
        CompositeFuture.all(serverClose, healthCheckClose).map((Void)null).setHandler(completionHandler);
    }
}
