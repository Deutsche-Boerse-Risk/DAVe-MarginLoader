package com.deutscheboerse.risk.dave.amqp;

import com.deutscheboerse.risk.dave.config.AmqpConfig;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.proton.*;

public class AmqpClient {
    private static final Logger LOG = LoggerFactory.getLogger(AmqpClient.class);

    private static final int FULL_FLOW_CREDIT = 1000;
    private static final int THROTTLE_FLOW_CREDIT = 1;

    private final Vertx vertx;
    private final AmqpConfig config;
    private final String verticleName;
    private final String queueName;
    private final ProtonClient protonClient;

    private ProtonConnection protonBrokerConnection;
    private ProtonReceiver protonBrokerReceiver;
    private boolean stopping;
    private Runnable flowRequest = null;

    private Handler<AsyncResult<Void>> connectHandler;
    private ProtonMessageHandler deliveryHandler;
    private Handler<Void> disconnectHandler;

    public AmqpClient(Vertx vertx, AmqpConfig config, String verticleName, String queueName) {
        this.vertx = vertx;
        this.config = config;
        this.protonClient = ProtonClient.create(vertx);
        this.verticleName = verticleName;
        this.queueName = queueName;
    }

    public void connect() {
        createBrokerConnection()
                .compose(i -> setupDisconnectHandler())
                .compose(i -> createAmqpReceiver())
                .setHandler(this.connectHandler);
    }

    public AmqpClient setConnectHandler(Handler<AsyncResult<Void>> handler) {
        this.connectHandler = handler;
        return this;
    }

    public AmqpClient setDeliveryHandler(ProtonMessageHandler handler) {
        this.deliveryHandler = handler;
        return this;
    }

    public AmqpClient setDisconnectHandler(Handler<Void> handler) {
        this.disconnectHandler = handler;
        return this;
    }

    public void stop() {
        stopping = true;
        flowRequest = null;
        protonBrokerReceiver.drain(0, res -> {
            stopping = false;
            if (flowRequest != null) {
                flowRequest.run();
                flowRequest = null;
            }
        });
    }

    public void throttle() {
        setFlowRequest(() -> protonBrokerReceiver.flow(THROTTLE_FLOW_CREDIT));
    }

    public void run() {
        setFlowRequest(() -> protonBrokerReceiver.flow(FULL_FLOW_CREDIT));
    }

    public void disconnect() {
        this.connectHandler = null;
        this.deliveryHandler = null;
        this.disconnectHandler = null;
        if (this.protonBrokerReceiver != null) {
            this.protonBrokerReceiver.close();
        }
        if (this.protonBrokerConnection != null) {
            this.protonBrokerConnection.disconnect();
        }
    }

    private Future<Void> createBrokerConnection() {
        Future<Void> createBrokerConnectionFuture = Future.future();

        Future<ProtonConnection> connectFuture = Future.future();
        protonClient.connect(getClientOptions(),
                this.config.getHostname(),
                this.config.getPort(),
                this.config.getUsername(),
                this.config.getPassword(),
                connectFuture);

        connectFuture.compose(connectResult -> {
            // When the connection is established (connectFuture), execute this
            Future<ProtonConnection> openFuture = Future.future();
            connectResult.setContainer("mdh/marketDataLoader-" + this.verticleName).openHandler(openFuture).open();
            return openFuture;
        }).compose(openResult -> {
            // When the connection is open (openFuture), execute this
            this.protonBrokerConnection = openResult;
            createBrokerConnectionFuture.complete();
        }, createBrokerConnectionFuture);

        return createBrokerConnectionFuture;
    }

    private ProtonClientOptions getClientOptions() {
        return new ProtonClientOptions()
                .setReconnectAttempts(this.config.getReconnectAttempts())
                .setReconnectInterval(this.config.getReconnectTimeout());
    }

    private Future<Void> setupDisconnectHandler() {
        this.protonBrokerConnection.disconnectHandler(connection -> {
            LOG.info("{} disconnected ", this.verticleName);
            if (this.protonBrokerReceiver != null) {
                this.protonBrokerReceiver.close();
            }
            if (disconnectHandler != null) {
                disconnectHandler.handle(null);
            }

            // Try to reconnect in a few seconds
            int timeout = this.config.getReconnectTimeout();
            vertx.setTimer(timeout, id -> connect());
        });
        return Future.succeededFuture();
    }

    private Future<Void> createAmqpReceiver() {
        Future<ProtonReceiver> receiverOpenFuture = Future.future();
        this.protonBrokerReceiver = this.protonBrokerConnection.createReceiver(this.queueName);
        this.protonBrokerReceiver.setPrefetch(0);
        this.protonBrokerReceiver.flow(FULL_FLOW_CREDIT);
        this.protonBrokerReceiver.setAutoAccept(false);
        this.protonBrokerReceiver.openHandler(receiverOpenFuture);
        this.protonBrokerReceiver.closeHandler(ar -> LOG.info("Closed"));
        this.protonBrokerReceiver.handler(this.deliveryHandler).open();
        return receiverOpenFuture.mapEmpty();
    }


    private void setFlowRequest(@Nullable Runnable request) {
        if (stopping) {
            flowRequest = request;
        } else {
            // Run immediately
            if (request != null) {
                request.run();
            }
            flowRequest = null;
        }
    }
}
