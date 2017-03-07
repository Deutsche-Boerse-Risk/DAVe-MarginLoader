package com.deutscheboerse.risk.dave.utils;

import CIL.ObjectList;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonSender;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;

import java.util.Collection;
import java.util.Optional;
import java.util.Random;
import java.util.function.BiFunction;

public class BrokerFillerInvalidProtocol extends BrokerFillerCorrectData {

    public BrokerFillerInvalidProtocol(Vertx vertx) {
        super(vertx);
    }

    @Override
    protected Future<ProtonConnection> populateQueue(ProtonConnection protonConnection, String queueName, String folderName, Collection<Integer> ttsaveNumbers, BiFunction<String, Integer, Optional<ObjectList.GPBObjectList>> gpbBuilder) {
        Future<ProtonConnection> populateQueueFuture = Future.future();
        protonConnection.createSender(queueName).openHandler(openResult -> {
            if (openResult.succeeded()) {
                ProtonSender sender = openResult.result();
                sender.setAutoSettle(true);
                for (Integer ignored : ttsaveNumbers) {
                    Message message = Message.Factory.create();
                    byte[] messageBytes = new byte[256];
                    new Random().nextBytes(messageBytes);
                    message.setBody(new Data(new Binary(messageBytes)));
                    sender.send(message);
                }
                populateQueueFuture.complete(protonConnection);
            } else {
                populateQueueFuture.fail(openResult.cause());
            }
        }).open();
        return populateQueueFuture;
    }

}
