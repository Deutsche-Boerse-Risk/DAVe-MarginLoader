package com.deutscheboerse.risk.dave;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonSender;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public class BrokerFiller {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerFiller.class);
    private final Vertx vertx;
    private final int tcpPort;
    private static ProtonConnection protonConnection;

    public BrokerFiller(Vertx vertx) {
        this.vertx = vertx;
        this.tcpPort = Integer.getInteger("cil.tcpport", 5672);
    }

    public void setUpAllQueues(Handler<AsyncResult<String>> handler) {
        Future<ProtonConnection> chainFuture = Future.future();
        this.createAmqpConnection()
                .compose(this::populateAccountMarginQueue)
                .compose(this::populateLiquiGroupMarginQueue)
                .compose(this::populateLiquiGroupSplitMarginQueue)
                .compose(this::populatePoolMarginQueue)
                .compose(this::populatePositionReportQueue)
                .compose(chainFuture::complete, chainFuture);
        chainFuture.setHandler(ar -> {
           if (ar.succeeded()) {
               handler.handle(Future.succeededFuture());
           } else {
               handler.handle(Future.failedFuture(ar.cause()));
           }
        });
    }

    public void setUpAccountMarginQueue(Handler<AsyncResult<String>> handler) {
        Future<ProtonConnection> chainFuture = Future.future();
        this.createAmqpConnection()
                .compose(this::populateAccountMarginQueue)
                .compose(chainFuture::complete, chainFuture);
        chainFuture.setHandler(ar -> {
           if (ar.succeeded()) {
               handler.handle(Future.succeededFuture());
           } else {
               handler.handle(Future.failedFuture(ar.cause()));
           }
        });
    }

    public void setUpLiquiGroupMarginQueue(Handler<AsyncResult<String>> handler) {
        Future<ProtonConnection> chainFuture = Future.future();
        this.createAmqpConnection()
                .compose(this::populateLiquiGroupMarginQueue)
                .compose(chainFuture::complete, chainFuture);
        chainFuture.setHandler(ar -> {
           if (ar.succeeded()) {
               handler.handle(Future.succeededFuture());
           } else {
               handler.handle(Future.failedFuture(ar.cause()));
           }
        });
    }

    public void setUpLiquiGroupSplitMarginQueue(Handler<AsyncResult<String>> handler) {
        Future<ProtonConnection> chainFuture = Future.future();
        this.createAmqpConnection()
                .compose(this::populateLiquiGroupSplitMarginQueue)
                .compose(chainFuture::complete, chainFuture);
        chainFuture.setHandler(ar -> {
           if (ar.succeeded()) {
               handler.handle(Future.succeededFuture());
           } else {
               handler.handle(Future.failedFuture(ar.cause()));
           }
        });
    }

    public void setUpPoolMarginQueue(Handler<AsyncResult<String>> handler) {
        Future<ProtonConnection> chainFuture = Future.future();
        this.createAmqpConnection()
                .compose(this::populatePoolMarginQueue)
                .compose(chainFuture::complete, chainFuture);
        chainFuture.setHandler(ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void setUpPositionReportQueue(Handler<AsyncResult<String>> handler) {
        Future<ProtonConnection> chainFuture = Future.future();
        this.createAmqpConnection()
                .compose(this::populatePositionReportQueue)
                .compose(chainFuture::complete, chainFuture);
        chainFuture.setHandler(ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private Future<ProtonConnection> createAmqpConnection() {
        Future<ProtonConnection> createAmqpConnectionFuture = Future.future();
        ProtonClient protonClient = ProtonClient.create(vertx);
        protonClient.connect("localhost", this.tcpPort, "admin", "admin", connectResult -> {
            if (connectResult.succeeded()) {
                connectResult.result().setContainer("dave/marginLoaderIT").openHandler(openResult -> {
                    if (openResult.succeeded()) {
                        BrokerFiller.protonConnection = openResult.result();
                        createAmqpConnectionFuture.complete(BrokerFiller.protonConnection);
                    } else {
                        createAmqpConnectionFuture.fail(openResult.cause());
                    }
                }).open();
            } else {
                createAmqpConnectionFuture.fail(connectResult.cause());
            }
        });
        return createAmqpConnectionFuture;
    }

    private Future<ProtonConnection> populateAccountMarginQueue(ProtonConnection protonConnection) {
        final String queueName = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEAccountMargin";
        final Collection<String> messagePaths = IntStream.rangeClosed(1, 1)
                .mapToObj(i -> String.format("%s/%03d.bin", BrokerFiller.class.getResource("accountMargin").getPath(), i))
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, queueName, messagePaths);
    }

    private Future<ProtonConnection> populateLiquiGroupMarginQueue(ProtonConnection protonConnection) {
        final String queueName = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupMargin";
        final Collection<String> messagePaths = IntStream.rangeClosed(1, 1)
                .mapToObj(i -> String.format("%s/%03d.bin", BrokerFiller.class.getResource("liquiGroupMargin").getPath(), i))
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, queueName, messagePaths);
    }

    private Future<ProtonConnection> populateLiquiGroupSplitMarginQueue(ProtonConnection protonConnection) {
        final String queueName = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupSplitMargin";
        final Collection<String> messagePaths = IntStream.rangeClosed(1, 1)
                .mapToObj(i -> String.format("%s/%03d.bin", BrokerFiller.class.getResource("liquiGroupSplitMargin").getPath(), i))
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, queueName, messagePaths);
    }

    private Future<ProtonConnection> populatePositionReportQueue(ProtonConnection protonConnection) {
        final String queueName = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEPositionReport";
        final Collection<String> messagePaths = IntStream.rangeClosed(1, 1)
                .mapToObj(i -> String.format("%s/%03d.bin", BrokerFiller.class.getResource("positionReport").getPath(), i))
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, queueName, messagePaths);
    }
    private Future<ProtonConnection> populatePoolMarginQueue(ProtonConnection protonConnection) {
        final String queueName = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEPoolMargin";
        final Collection<String> messagePaths = IntStream.rangeClosed(1, 1)
                .mapToObj(i -> String.format("%s/%03d.bin", BrokerFiller.class.getResource("poolMargin").getPath(), i))
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, queueName, messagePaths);
    }
    private Future<ProtonConnection> populateRiskLimitUtilizationQueue(ProtonConnection protonConnection) {
        final String queueName = "";
        final Collection<String> messagePaths = IntStream.rangeClosed(1, 1)
                .mapToObj(i -> String.format("%s/%03d.bin", BrokerFiller.class.getResource("riskLimitUtilization").getPath(), i))
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, queueName, messagePaths);
    }

    private Future<ProtonConnection> populateQueue(ProtonConnection protonConnection, String queueName, Collection<String> messagePaths) {
        Future<ProtonConnection> populateQueueFuture = Future.future();
        protonConnection.createSender(queueName).openHandler(openResult -> {
            if (openResult.succeeded()) {
                boolean allSent = true;
                ProtonSender sender = openResult.result();
                sender.setAutoSettle(true);
                for (String messagePath : messagePaths) {
                    try {
                        Message message = Message.Factory.create();
                        byte[] messageBytes = Files.readAllBytes(Paths.get(messagePath));
                        message.setBody(new Data(new Binary(messageBytes)));
                        sender.send(message);
                    } catch (IOException e) {
                        e.printStackTrace();
                        allSent = false;
                    }
                }
                if (allSent) {
                    populateQueueFuture.complete(protonConnection);
                } else {
                    populateQueueFuture.fail("Failed to send some messages to " + queueName);
                }
            } else {
                populateQueueFuture.fail(openResult.cause());
            }
        }).open();
        return populateQueueFuture;
    }

}
