package com.deutscheboerse.risk.dave.utils;

import CIL.ObjectList;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonSender;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;

import java.util.Collection;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class BrokerFillerWrongBody extends BrokerFillerCorrectData {

    public BrokerFillerWrongBody(Vertx vertx) {
        super(vertx);
    }

    @Override
    protected Future<ProtonConnection> populateQueue(ProtonConnection protonConnection, String queueName,  Collection<Integer> ttsaveNumbers, Function<Integer, Optional<ObjectList.GPBObjectList>> gpbBuilder) {
        Future<ProtonConnection> populateQueueFuture = Future.future();
        protonConnection.createSender(queueName).openHandler(openResult -> {
            if (openResult.succeeded()) {
                ProtonSender sender = openResult.result();
                sender.setAutoSettle(true);
                for (Integer ignored : ttsaveNumbers) {
                    Message message = Message.Factory.create();
                    message.setBody(new AmqpValue("Wrong body"));
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
