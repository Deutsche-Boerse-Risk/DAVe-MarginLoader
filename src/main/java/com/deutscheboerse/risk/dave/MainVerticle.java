package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MainVerticle extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(MainVerticle.class);
    private static final String MONGO_CONF = "mongo";
    private static final String BROKER_CONF = "broker";
    private static final String HEALTHCHECK_CONF = "healthCheck";
    private Map<String, String> verticleDeployments = new HashMap<>();

    @Override
    public void start(Future<Void> startFuture) {
        HealthCheck healthCheck = new HealthCheck(this.vertx);

        Future<Void> chainFuture = Future.future();
        this.deployPersistenceVerticle()
                .compose(i -> deployAccountMarginVerticle())
                .compose(i -> deployLiquiGroupMarginVerticle())
                .compose(i -> deployLiquiGroupSplitMarginVerticle())
                .compose(i -> deployPoolMarginVerticle())
                .compose(i -> deployHealthCheckVerticle())
                .compose(chainFuture::complete, chainFuture);

        chainFuture.setHandler(ar -> {
            if (ar.succeeded()) {
                LOG.info("All verticles deployed");
                healthCheck.setMainState(true);
                startFuture.complete();
            } else {
                LOG.error("Fail to deploy some verticle");
                healthCheck.setMainState(false);
                closeAllDeployments();
                startFuture.fail(chainFuture.cause());
            }
        });
    }

    private Future<Void> deployPersistenceVerticle() {
        return this.deployVerticle(PersistenceVerticle.class, config().getJsonObject(MONGO_CONF, new JsonObject()));
    }

    private Future<Void> deployAccountMarginVerticle() {
        return this.deployVerticle(AccountMarginVerticle.class, config().getJsonObject(BROKER_CONF, new JsonObject()));
    }

    private Future<Void> deployLiquiGroupMarginVerticle() {
        return this.deployVerticle(LiquiGroupMarginVerticle.class, config().getJsonObject(BROKER_CONF, new JsonObject()));
    }

    private Future<Void> deployLiquiGroupSplitMarginVerticle() {
        return this.deployVerticle(LiquiGroupSplitMarginVerticle.class, config().getJsonObject(BROKER_CONF, new JsonObject()));
    }

    private Future<Void> deployPoolMarginVerticle() {
        return this.deployVerticle(PoolMarginVerticle.class, config().getJsonObject(BROKER_CONF, new JsonObject()));
    }

    private Future<Void> deployHealthCheckVerticle() {
        return this.deployVerticle(HealthCheckVerticle.class, config().getJsonObject(HEALTHCHECK_CONF, new JsonObject()));
    }

    private Future<Void> deployVerticle(Class clazz, JsonObject config) {
        Future<Void> verticleFuture = Future.future();
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        vertx.deployVerticle(clazz.getName(), options, ar -> {
            if (ar.succeeded()) {
                LOG.info("Deployed {} with ID {}", clazz.getName(), ar.result());
                verticleDeployments.put(clazz.getSimpleName(), ar.result());
                verticleFuture.complete();
            } else {
                verticleFuture.fail(ar.cause());
            }
        });
        return verticleFuture;
    }

    private void closeAllDeployments() {
        LOG.info("Undeploying verticles");

        List<Future> futures = new LinkedList<>();
        this.verticleDeployments.forEach((verticleName, deploymentID) -> {
            if (deploymentID != null && vertx.deploymentIDs().contains(deploymentID)) {
                LOG.info("Undeploying {} with ID: {}", verticleName, deploymentID);
                Future<Void> future = Future.future();
                vertx.undeploy(deploymentID, future.completer());
                futures.add(future);
            }
        });

        CompositeFuture.all(futures).setHandler(ar -> {
            if (ar.succeeded()) {
                LOG.info("Undeployed all verticles");
            } else {
                LOG.error("Failed to undeploy some verticles", ar.cause());
            }
        });
    }

    @Override
    public void stop() throws Exception {
        LOG.info("Stopping main verticle");
        this.closeAllDeployments();
        super.stop();
    }

}
