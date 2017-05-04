package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.amqp.AmqpClient;
import com.deutscheboerse.risk.dave.config.AmqpConfig;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import com.deutscheboerse.risk.dave.model.AbstractModel;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Extension;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.proton.ProtonDelivery;
import io.vertx.serviceproxy.ProxyHelper;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.messaging.Section;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

public abstract class AMQPVerticle<GPBType extends Message, ModelType extends AbstractModel>
        extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(AMQPVerticle.class);
    private static final int DEFAULT_PROXY_SEND_TIMEOUT = 60000;
    private static final ExtensionRegistry registry = ExtensionRegistry.newInstance();
    static {
        PrismaReports.registerAllExtensions(registry);
    }

    private final String verticleName = this.getClass().getSimpleName();
    private AmqpClient amqpClient;
    private PersistenceService persistenceService;
    private HealthCheck healthCheck;
    private AmqpConfig config;
    private HealthCheck.Component healthCheckComponent;
    private Extension<ObjectList.GPBObject, GPBType> gpbExtension;
    private BiFunction<PrismaReports.PrismaHeader, GPBType, ModelType> modelFactory;
    private CircuitBreaker circuitBreaker;

    @Override
    public void start() throws IOException {
        LOG.info("Starting {} with configuration: {}", verticleName, config().encodePrettily());

        DeliveryOptions deliveryOptions = new DeliveryOptions().setSendTimeout(DEFAULT_PROXY_SEND_TIMEOUT);
        this.persistenceService = ProxyHelper.createProxy(PersistenceService.class, vertx, PersistenceService.SERVICE_ADDRESS, deliveryOptions);
        this.healthCheck = new HealthCheck(this.vertx);
        this.config = (new ObjectMapper()).readValue(config().toString(), AmqpConfig.class);
        this.amqpClient = new AmqpClient(this.vertx, this.config, this.verticleName, this.getAmqpQueueName());
        this.healthCheckComponent = this.getHealthCheckComponent();
        this.gpbExtension = this.getGpbExtension();
        this.modelFactory = this.getModelFactory();
        this.circuitBreaker = this.createCircuitBreaker(this.vertx);

        // This is asynchronous call, verticle will try to connect to the broker on background.
        this.connect();

        LOG.info("{} deployed", this.verticleName);
    }

    protected abstract HealthCheck.Component getHealthCheckComponent();
    protected abstract Extension<ObjectList.GPBObject, GPBType> getGpbExtension();
    protected abstract BiFunction<PrismaReports.PrismaHeader, GPBType, ModelType> getModelFactory();
    protected abstract String getAmqpQueueName();

    protected abstract void store(List<ModelType> models, Handler<AsyncResult<Void>> handler);

    private void connect() {
        this.amqpClient
                .setDeliveryHandler(this::processDelivery)
                .setDisconnectHandler(i ->
                        healthCheck.setComponentFailed(this.healthCheckComponent)
                ).setConnectHandler(ar -> {
            if (ar.succeeded()) {
                LOG.info("{} connected to the broker", this.verticleName);
                healthCheck.setComponentReady(this.healthCheckComponent);
            } else {
                LOG.error("{} failed to connect", this.verticleName);
            }
        }).connect();
    }

    private CircuitBreaker createCircuitBreaker(Vertx vertx) {
        return CircuitBreaker.create("amqp-circuit-breaker", vertx,
                new CircuitBreakerOptions()
                        .setMaxFailures(config.getCircuitBreaker().getMaxFailures())
                        .setTimeout(config.getCircuitBreaker().getTimeout())
                        .setResetTimeout(config.getCircuitBreaker().getResetTimeout())
        ).openHandler(i -> {
            LOG.warn("Open circuit");
            amqpClient.stop();
        }).halfOpenHandler(i -> {
            LOG.warn("Half open circuit");
            amqpClient.throttle();
        }).closeHandler(i -> {
            LOG.warn("Close circuit");
            amqpClient.run();
        });
    }

    private void processDelivery(ProtonDelivery delivery, org.apache.qpid.proton.message.Message msg) {
        Optional<ObjectList.GPBObjectList> gpbObjectList = parseObjectList(msg.getBody());
        if (gpbObjectList.isPresent()) {
            this.processObjectList(gpbObjectList.get(), ar -> {
                PrismaReports.PrismaHeader header = gpbObjectList.get().getHeader().getExtension(PrismaReports.prismaHeader);
                if (ar.succeeded()) {
                    LOG.info("Message settled: {} (ttsave={}, {} of {})", this.verticleName, header.getId(), header.getMessageId(), header.getMessageCount());
                    delivery.settle();
                } else {
                    LOG.warn("Message released: {} (ttsave={} {} of {})", this.verticleName, header.getId(), header.getMessageId(), header.getMessageCount(), ar.cause());
                    delivery.disposition(new Released(), true);
                }
                amqpClient.increaseCreditBy(1);
            });
        } else {
            LOG.info("Message settled: skipping invalid message");
            delivery.settle();
            amqpClient.increaseCreditBy(1);
        }
    }

    private Optional<ObjectList.GPBObjectList> parseObjectList(Section messageBody) {
        if (!(messageBody instanceof Data)) {
            LOG.error("Incoming message's body is not a 'data' type, skipping ... ");
        } else {
            Binary bin = ((Data) messageBody).getValue();
            try {
                ObjectList.GPBObjectList gpbObjectList = ObjectList.GPBObjectList.parseFrom(bin.getArray(), registry);
                LOG.debug(String.format("Parsed %d item(s)", gpbObjectList.getItemCount()));
                if (gpbObjectList.hasHeader() && gpbObjectList.getHeader().hasExtension(PrismaReports.prismaHeader)) {
                    return Optional.of(gpbObjectList);
                } else {
                    LOG.error("Message header is missing for message - ignoring it (Verticle: {})", this.verticleName);
                }
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Unable to decode GPB message", e);
            }
        }
        return Optional.empty();
    }

    private void processObjectList(ObjectList.GPBObjectList gpbObjectList, Handler<AsyncResult<Void>> handler) {
        PrismaReports.PrismaHeader header = gpbObjectList.getHeader().getExtension(PrismaReports.prismaHeader);
        List<ModelType> dataModels = new ArrayList<>(gpbObjectList.getItemList().size());
        gpbObjectList.getItemList().forEach(gpbObject -> {
            if (gpbObject.hasExtension(gpbExtension)) {
                GPBType gpbData = gpbObject.getExtension(gpbExtension);
                try {
                    dataModels.add(this.modelFactory.apply(header, gpbData));
                } catch (IllegalArgumentException ex) {
                    LOG.error("Unable to create Data Model from GPB data", ex);
                }
            } else {
                LOG.error("Unknown extension (should be {})", gpbExtension.getDescriptor().getName());
            }
        });
        storeDataModel(dataModels, handler);
    }

    private void storeDataModel(List<ModelType> dataModels, Handler<AsyncResult<Void>> handler) {
        circuitBreaker.execute(future ->
            this.store(dataModels, ar -> {
                if (ar.succeeded()) {
                    LOG.debug("Message processed");
                    future.complete();
                } else {
                    LOG.error("Unable to store message", ar.cause());
                    future.fail(ar.cause());
                }
            })
        ).map((Void)null).setHandler(handler);
    }

    protected PersistenceService getPersistenceService() {
        return this.persistenceService;
    }

    protected AmqpConfig getAmqpConfig() {
        return this.config;
    }

    @Override
    public void stop() {
        amqpClient.disconnect();
    }
}
