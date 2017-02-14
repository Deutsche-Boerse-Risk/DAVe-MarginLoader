package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

/**
 * Starts an {@link HttpServer} on default port 8080.
 * <p>
 * It exports these two web services:
 * <ul>
 *   <li>/healthz   - Always replies "ok" (provided the web server is running)
 *   <li>/readiness - Replies "ok" or "nok" indicating whether all verticles
 *                    are up and running
 * </ul>
 */
public class HttpVerticle extends AbstractVerticle
{
    private static final Logger LOG = LoggerFactory.getLogger(HttpVerticle.class);

    private static final Integer DEFAULT_PORT = 8080;

    private HttpServer server;
    private HealthCheck healthCheck;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        LOG.info("Starting {} with configuration: {}", HttpVerticle.class.getSimpleName(), config().encodePrettily());

        healthCheck = new HealthCheck(this.vertx);

        startHttpServer().setHandler(ar -> {
            if (ar.succeeded()) {
                startFuture.complete();
            }
            else {
                startFuture.fail(ar.cause());
            }
        });
    }

    private Future<HttpServer> startHttpServer() {
        Future<HttpServer> webServerFuture = Future.future();
        Router router = configureRouter();

        int port = config().getInteger("port", HttpVerticle.DEFAULT_PORT);

        LOG.info("Starting web server on port {}", port);
        server = vertx.createHttpServer()
                .requestHandler(router::accept)
                .listen(port, webServerFuture.completer());

        return webServerFuture;
    }

    private Router configureRouter() {

        Router router = Router.router(vertx);

        LOG.info("Adding route REST API");
        router.get("/healthz").handler(this::healthz);
        router.get("/readiness").handler(this::readiness);

        return router;
    }

    private void healthz(RoutingContext routingContext) {
        routingContext.response().setStatusCode(200).end("ok");
    }

    private void readiness(RoutingContext routingContext) {
        if (healthCheck.ready())  {
            routingContext.response().setStatusCode(200).end("ok");
        } else  {
            routingContext.response().setStatusCode(503).end("nok");
        }
    }

    @Override
    public void stop() throws Exception {
        LOG.info("Shutting down webserver");
        server.close();
    }
}
