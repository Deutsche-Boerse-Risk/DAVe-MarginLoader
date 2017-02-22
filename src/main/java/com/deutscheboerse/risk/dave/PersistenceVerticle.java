package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.persistence.MongoPersistenceService;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.serviceproxy.ProxyHelper;


public class PersistenceVerticle extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(PersistenceVerticle.class);
    private MessageConsumer<JsonObject> persistenceServiceConsumer;

    @Override
    public void start(Future<Void> fut) throws Exception {
        LOG.info("Starting {} with configuration: {}", PersistenceVerticle.class.getSimpleName(), config().encodePrettily());

        PersistenceService persistenceService = new MongoPersistenceService(vertx);
        this.persistenceServiceConsumer = ProxyHelper.registerService(PersistenceService.class, vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        persistenceService.initialize(ar -> {
            if (ar.succeeded()) {
                LOG.info("Persistence verticle started");
                fut.complete();
            } else {
                LOG.error("Persistence verticle failed to deploy", ar.cause());
                fut.fail(ar.cause());
            }
        });
    }

    @Override
    public void stop() throws Exception {
        ProxyHelper.unregisterService(this.persistenceServiceConsumer);
        super.stop();
    }

}
