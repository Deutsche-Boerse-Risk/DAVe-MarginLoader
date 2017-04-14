package com.deutscheboerse.risk.dave;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
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
    private static final String STORE_MANAGER_CONF_KEY = "storeManager";
    private static final String AMQP_CONF_KEY = "amqp";
    private static final String HEALTHCHECK_CONF_KEY = "healthCheck";
    private static final String GUICE_BINDER_KEY = "guice_binder";
    private JsonObject configuration;
    private Map<String, String> verticleDeployments = new HashMap<>();

    @Override
    public void start(Future<Void> startFuture) {
        Future<Void> chainFuture = Future.future();
        this.retrieveConfig()
                .compose(i -> deployPersistenceVerticle())
                .compose(i -> deployAccountMarginVerticle())
                .compose(i -> deployLiquiGroupMarginVerticle())
                .compose(i -> deployLiquiGroupSplitMarginVerticle())
                .compose(i -> deployPoolMarginVerticle())
                .compose(i -> deployPositionReportVerticle())
                .compose(i -> deployRiskLimitUtilizationVerticle())
                .compose(i -> deployHealthCheckVerticle())
                .compose(chainFuture::complete, chainFuture);

        chainFuture.setHandler(ar -> {
            if (ar.succeeded()) {
                LOG.info("All verticles deployed");
                startFuture.complete();
            } else {
                LOG.error("Fail to deploy some verticle");
                closeAllDeployments();
                startFuture.fail(chainFuture.cause());
            }
        });
    }

    private void addHoconConfigStoreOptions(ConfigRetrieverOptions options) {
        String configurationFile = System.getProperty("dave.configurationFile");
        if (configurationFile != null) {
            options.addStore(new ConfigStoreOptions()
                    .setType("file")
                    .setFormat("hocon")
                    .setConfig(new JsonObject()
                            .put("path", configurationFile)));
        }
    }

    private void addDeploymentConfigStoreOptions(ConfigRetrieverOptions options) {
        options.addStore(new ConfigStoreOptions().setType("json").setConfig(vertx.getOrCreateContext().config()));
    }

    private static JsonObject implicitConfigTypeConversion(JsonObject json) {
        json.forEach(entry -> {
            Object value = entry.getValue();
            if (value instanceof String) {
                String s = (String)value;
                if (s.matches("^-?\\d+$")) {
                    json.put(entry.getKey(), Integer.valueOf(s));
                } else if (s.matches("^(true)|(false)$")) {
                    json.put(entry.getKey(), Boolean.valueOf(s));
                }
            } else if (value instanceof JsonObject) {
                implicitConfigTypeConversion(json.getJsonObject(entry.getKey()));
            }
        });
        return json;
    }

    private Future<Void> retrieveConfig() {
        Future<Void> future = Future.future();
        ConfigRetrieverOptions options = new ConfigRetrieverOptions();
            this.addHoconConfigStoreOptions(options);
            this.addDeploymentConfigStoreOptions(options);
            ConfigRetriever retriever = ConfigRetriever.create(vertx, options);
            retriever.getConfig(ar -> {
                if (ar.succeeded()) {
                    this.configuration = implicitConfigTypeConversion(ar.result());
                    LOG.debug("Retrieved configuration: {}", this.configuration.encodePrettily());
                    future.complete();
                } else {
                    LOG.error("Unable to retrieve configuration", ar.cause());
                    future.fail(ar.cause());
                }
            });
        return future;
    }

    private Future<Void> deployPersistenceVerticle() {
        return this.deployVerticle(PersistenceVerticle.class, this.configuration.getJsonObject(STORE_MANAGER_CONF_KEY, new JsonObject())
            .put(GUICE_BINDER_KEY, this.configuration.getString(GUICE_BINDER_KEY, PersistenceBinder.class.getName())));
    }

    private Future<Void> deployAccountMarginVerticle() {
        return this.deployVerticle(AccountMarginVerticle.class, this.configuration.getJsonObject(AMQP_CONF_KEY, new JsonObject()));
    }

    private Future<Void> deployLiquiGroupMarginVerticle() {
        return this.deployVerticle(LiquiGroupMarginVerticle.class, this.configuration.getJsonObject(AMQP_CONF_KEY, new JsonObject()));
    }

    private Future<Void> deployLiquiGroupSplitMarginVerticle() {
        return this.deployVerticle(LiquiGroupSplitMarginVerticle.class, this.configuration.getJsonObject(AMQP_CONF_KEY, new JsonObject()));
    }

    private Future<Void> deployPoolMarginVerticle() {
        return this.deployVerticle(PoolMarginVerticle.class, this.configuration.getJsonObject(AMQP_CONF_KEY, new JsonObject()));
    }

    private Future<Void> deployPositionReportVerticle() {
        return this.deployVerticle(PositionReportVerticle.class, this.configuration.getJsonObject(AMQP_CONF_KEY, new JsonObject()));
    }

    private Future<Void> deployRiskLimitUtilizationVerticle() {
        return this.deployVerticle(RiskLimitUtilizationVerticle.class, this.configuration.getJsonObject(AMQP_CONF_KEY, new JsonObject()));
    }

    private Future<Void> deployHealthCheckVerticle() {
        return this.deployVerticle(HealthCheckVerticle.class, this.configuration.getJsonObject(HEALTHCHECK_CONF_KEY, new JsonObject()));
    }

    private Future<Void> deployVerticle(Class clazz, JsonObject config) {
        Future<Void> verticleFuture = Future.future();
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        String prefix = config.containsKey(GUICE_BINDER_KEY) ? "java-guice:" : "java:";
        vertx.deployVerticle(prefix + clazz.getName(), options, ar -> {
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
