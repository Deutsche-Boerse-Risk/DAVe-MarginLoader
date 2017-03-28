package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonClientOptions;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import io.vertx.serviceproxy.ProxyHelper;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;

public abstract class AMQPVerticle extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(AMQPVerticle.class);
    private static final String DEFAULT_BROKER_HOST = "localhost";
    private static final int DEFAULT_BROKER_PORT = 5672;

    private static final int DEFAULT_RECONNECT_ATTEMPTS = -1;
    private static final int DEFAULT_RECONNECT_TIMEOUT = 60000;

    private final ExtensionRegistry registry = ExtensionRegistry.newInstance();
    private final String verticleName = this.getClass().getSimpleName();
    private ProtonClient protonClient;
    private ProtonConnection protonBrokerConnection;
    private ProtonReceiver protonBrokerReceiver;
    protected PersistenceService persistenceService;
    protected HealthCheck healthCheck;

    @Override
    public void start(Future<Void> fut) {
        LOG.info("Starting {} with configuration: {}", verticleName, config().encodePrettily());
        this.registerExtensions();

        this.persistenceService = ProxyHelper.createProxy(PersistenceService.class, vertx, PersistenceService.SERVICE_ADDRESS);
        this.healthCheck = new HealthCheck(this.vertx);
        this.protonClient = ProtonClient.create(vertx);

        // This is asynchronous call, verticle will try to connect to the broker on background.
        connect();

        LOG.info("{} deployed", this.verticleName);
        fut.complete();
    }

    protected abstract String getAmqpContainerName();
    protected abstract String getAmqpQueueName();
    protected abstract void onConnect();
    protected abstract void onDisconnect();
    protected abstract void processObjectList(ObjectList.GPBObjectList objectList);

    private void registerExtensions() {
        PrismaReports.registerAllExtensions(this.registry);
    }

    private void connect() {
        createBrokerConnection()
                .compose(i -> setupDisconnectHandler())
                .compose(i -> createAmqpReceiver())
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        LOG.info("{} connected to the broker", this.verticleName);
                        // Notify subclasses that we are connected
                        onConnect();
                    } else {
                        LOG.error("{} failed to connect", this.verticleName);
                    }
                });
    }

    private ProtonClientOptions getClientOptions() {
        return new ProtonClientOptions()
                .setReconnectAttempts(config().getInteger("reconnectAttempts", DEFAULT_RECONNECT_ATTEMPTS))
                .setReconnectInterval(config().getInteger("reconnectTimeout", DEFAULT_RECONNECT_TIMEOUT));
    }

    private Future<Void> createBrokerConnection() {
        Future<Void> createBrokerConnectionFuture = Future.future();

        JsonObject amqpConfig = config();
        Future<ProtonConnection> connectFuture = Future.future();
        protonClient.connect(getClientOptions(),
                amqpConfig.getString("hostname", AMQPVerticle.DEFAULT_BROKER_HOST),
                amqpConfig.getInteger("port", AMQPVerticle.DEFAULT_BROKER_PORT),
                amqpConfig.getString("username"),
                amqpConfig.getString("password"),
                connectFuture);

        connectFuture.compose(connectResult -> {
            // When the connection is established (connectFuture), execute this
            Future<ProtonConnection> openFuture = Future.future();
            connectResult.setContainer(this.getAmqpContainerName()).openHandler(openFuture).open();
            return openFuture;
        }).compose(openResult -> {
            // When the connection is open (openFuture), execute this
            this.protonBrokerConnection = openResult;
            createBrokerConnectionFuture.complete();
        }, createBrokerConnectionFuture);

        return createBrokerConnectionFuture;
    }

    private Future<Void> setupDisconnectHandler() {
        this.protonBrokerConnection.disconnectHandler(connection -> {
            LOG.info("{} disconnected ", this.verticleName);
            if (this.protonBrokerReceiver != null) {
                this.protonBrokerReceiver.close();
            }
            // Notify subclasses that we were disconnected
            onDisconnect();
            // Try to reconnect in a few seconds
            int timeout = config().getInteger("reconnectTimeout", DEFAULT_RECONNECT_ATTEMPTS);
            vertx.setTimer(timeout, id -> connect());
        });
        return Future.succeededFuture();
    }

    private Future<Void> createAmqpReceiver() {
        Future<ProtonReceiver> receiverOpenFuture = Future.future();
        this.protonBrokerReceiver = this.protonBrokerConnection.createReceiver(this.getAmqpQueueName());
        this.protonBrokerReceiver.setPrefetch(1000);
        this.protonBrokerReceiver.setAutoAccept(false);
        this.protonBrokerReceiver.openHandler(receiverOpenFuture);
        this.protonBrokerReceiver.closeHandler(ar -> LOG.info("Closed"));
        this.protonBrokerReceiver.handler((delivery, msg) -> {
            Section body = msg.getBody();
            this.processMessage(body);
            delivery.settle();
        }).open();
        return receiverOpenFuture.mapEmpty();
    }

    private void processMessage(Section body) {
        if (!(body instanceof Data)) {
            LOG.error("Incoming message's body is not a 'data' type, skipping ... ");
        } else {
            Binary bin = ((Data) body).getValue();
            try {
                ObjectList.GPBObjectList gpbObjectList = ObjectList.GPBObjectList.parseFrom(bin.getArray(), this.registry);
                LOG.debug(String.format("Parsed %d item(s)", gpbObjectList.getItemCount()));
                if (gpbObjectList.hasHeader() && gpbObjectList.getHeader().hasExtension(PrismaReports.prismaHeader)) {
                    this.processObjectList(gpbObjectList);
                } else {
                    LOG.error("Message header is missing for message - ignoring it (Verticle: {})", this.verticleName);
                }
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Unable to decode GPB message", e);
            }
        }
    }

    @Override
    public void stop() throws Exception {
        if (this.protonBrokerConnection != null) {
            this.protonBrokerConnection.disconnect();
        }
        super.stop();
    }
}
