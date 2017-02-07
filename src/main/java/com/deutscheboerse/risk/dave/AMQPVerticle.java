package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.messaging.Section;

public abstract class AMQPVerticle extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(AMQPVerticle.class);
    private static final String DEFAULT_BROKER_HOST = "localhost";
    private static final int DEFAULT_BROKER_PORT = 5672;
    private static final String DEFAULT_BROKER_USER = "admin";
    private static final String DEFAULT_BROKER_PASSWORD = "admin";

    protected ExtensionRegistry registry = ExtensionRegistry.newInstance();
    protected ProtonConnection protonBrokerConnection;
    protected ProtonReceiver protonBrokerReceiver;

    @Override
    public void start(Future<Void> fut) throws Exception {
        this.registerExtensions();

        Future<Void> chainFuture = Future.future();
        createBrokerConnection()
                .compose(this::createAmqpReceiver)
                .compose(chainFuture::complete, chainFuture);
        chainFuture.setHandler(fut.completer());
    }

    protected abstract void registerExtensions();
    protected abstract String getAmqpContainerName();
    protected abstract String getAmqpQueueName();
    protected abstract void processObjectList(ObjectList.GPBObjectList objectList, Handler<AsyncResult<Void>> asyncResult);

    private Future<Void> createBrokerConnection() {
        Future<Void> createBrokerConnectionFuture = Future.future();
        Future<ProtonConnection> protonConnectionFuture = Future.future();
        protonConnectionFuture.setHandler(connectResult -> {
            if (connectResult.succeeded()) {
                connectResult.result().setContainer(this.getAmqpContainerName()).openHandler(openResult -> {
                    if (openResult.succeeded()) {
                        this.protonBrokerConnection = openResult.result();
                        createBrokerConnectionFuture.complete();
                    } else {
                        createBrokerConnectionFuture.fail(openResult.cause());
                    }
                }).open();
            } else {
                createBrokerConnectionFuture.fail(connectResult.cause());
            }
        });
        ProtonClient protonClient = ProtonClient.create(vertx);
        protonClient.connect(config().getJsonObject("broker", new JsonObject()).getString("hostname", AMQPVerticle.DEFAULT_BROKER_HOST),
                config().getJsonObject("broker", new JsonObject()).getInteger("port", AMQPVerticle.DEFAULT_BROKER_PORT),
                config().getJsonObject("broker", new JsonObject()).getString("username", AMQPVerticle.DEFAULT_BROKER_USER),
                config().getJsonObject("broker", new JsonObject()).getString("password", AMQPVerticle.DEFAULT_BROKER_PASSWORD),
                protonConnectionFuture.completer());
        return createBrokerConnectionFuture;
    }

    protected Future<Void> createAmqpReceiver(Void unused) {
        Future<Void> receiverOpenFuture = Future.future();
        this.protonBrokerReceiver = this.protonBrokerConnection.createReceiver(this.getAmqpQueueName());
        this.protonBrokerReceiver.setPrefetch(1000);
        this.protonBrokerReceiver.setAutoAccept(false);
        this.protonBrokerReceiver.openHandler(ar -> {
            if (ar.succeeded()) {
                receiverOpenFuture.complete();
            } else {
                receiverOpenFuture.fail(ar.cause());
            }
        });
        this.protonBrokerReceiver.closeHandler(ar -> {
            LOG.info("Closed");
        });
        this.protonBrokerReceiver.handler((delivery, msg) -> {
            Section body = msg.getBody();
            this.processMessage(body, ar -> {
                if (ar.succeeded()) {
                    LOG.debug("Message has been processed - will be settled");
                    delivery.settle();
                } else {
                    LOG.warn("Failed to store the message - will be released", ar.cause());
                    delivery.disposition(new Released(), true);
                }
            });
        }).open();
        return receiverOpenFuture;
    }

    private void processMessage(Section body, Handler<AsyncResult<Void>> asyncResult) {
        if (!(body instanceof Data)) {
            LOG.warn("Incoming message's body is not a 'data' type, skipping ... ");
            asyncResult.handle(Future.failedFuture("Message's body is not a 'data' type"));
        } else {
            Binary bin = ((Data) body).getValue();
            //this.saveMessage(bin);
            try {
                ObjectList.GPBObjectList gpbObjectList = ObjectList.GPBObjectList.parseFrom(bin.getArray(), this.registry);
                LOG.debug(String.format("Parsed %d item(s)", gpbObjectList.getItemCount()));
                if (!gpbObjectList.hasHeader() || !gpbObjectList.getHeader().hasExtension(PrismaReports.prismaHeader)) {
                    // Message header is missing - acknowledge only
                    LOG.warn("Message header is missing for message - ignoring it " + gpbObjectList.toString());
                    asyncResult.handle(Future.failedFuture("Message header is missing for message"));
                    return;
                }
                this.processObjectList(gpbObjectList, asyncResult);
            } catch (InvalidProtocolBufferException e) {
                asyncResult.handle(Future.failedFuture(e));
            }
        }
    }

    @Override
    public void stop() throws Exception {
        this.protonBrokerConnection.disconnect();
        super.stop();
    }
}
