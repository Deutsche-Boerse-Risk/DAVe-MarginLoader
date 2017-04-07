package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.utils.TestConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.junit.Test;

public class StoreManagerMock {
    private static final Logger LOG = LoggerFactory.getLogger(StoreManagerMock.class);

    private static final Integer DEFAULT_PORT = 8084;

    private final Vertx vertx;
    private final JsonObject config;
    private final HttpServer server;
    private boolean health = true;

    StoreManagerMock(Vertx vertx, JsonObject config) {
        this.vertx = vertx;
        this.config = config;
        this.server = this.createHttpServer();
    }

    StoreManagerMock listen(Handler<AsyncResult<Void>> resultHandler) {

        int port = config.getInteger("port", DEFAULT_PORT);
        LOG.info("Starting web server on port {}", port);

        Future<HttpServer> listenFuture = Future.future();
        server.listen(port, listenFuture);

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
        HealthCheckHandler healthCheckHandler = HealthCheckHandler.create(vertx);

        healthCheckHandler.register("healthz", this::healthz);

        Router router = Router.router(vertx);

        JsonObject restApi = config.getJsonObject("restApi", new JsonObject());

        LOG.info("Adding route REST API");
        router.get("/healthz").handler(healthCheckHandler);
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
        server.close(completionHandler);
    }
}
