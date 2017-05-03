package com.deutscheboerse.risk.dave.utils;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.config.AmqpConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonSender;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BrokerFillerCorrectData implements BrokerFiller {
    private final Vertx vertx;
    private final int tcpPort;
    private final AmqpConfig config;
    private static ProtonConnection protonConnection;

    public BrokerFillerCorrectData(Vertx vertx) {
        this.vertx = vertx;
        this.tcpPort = TestConfig.BROKER_PORT;
        try {
            this.config = (new ObjectMapper()).readValue(TestConfig.getAmqpConfig().toString(), AmqpConfig.class);
        } catch (IOException e) {
            throw new AssertionError("Jackson error", e);
        }
    }

    public void close() {
        protonConnection.close();
    }

    @Override
    public void setUpAllQueues(Handler<AsyncResult<String>> handler) {
        this.createAmqpConnection()
                .compose(this::populateAccountMarginQueue)
                .compose(this::populateLiquiGroupMarginQueue)
                .compose(this::populateLiquiGroupSplitMarginQueue)
                .compose(this::populatePoolMarginQueue)
                .compose(this::populatePositionReportQueue)
                .compose(this::populateRiskLimitUtilizationQueue)
                .map((String)null)
                .setHandler(handler);
    }

    @Override
    public void setUpAccountMarginQueue(Handler<AsyncResult<String>> handler) {
        setUpQueue(this::populateAccountMarginQueue, handler);
    }

    @Override
    public void setUpLiquiGroupMarginQueue(Handler<AsyncResult<String>> handler) {
        setUpQueue(this::populateLiquiGroupMarginQueue, handler);
    }

    @Override
    public void setUpLiquiGroupSplitMarginQueue(Handler<AsyncResult<String>> handler) {
        setUpQueue(this::populateLiquiGroupSplitMarginQueue, handler);
    }

    @Override
    public void setUpPoolMarginQueue(Handler<AsyncResult<String>> handler) {
        setUpQueue(this::populatePoolMarginQueue, handler);
    }

    @Override
    public void setUpPositionReportQueue(Handler<AsyncResult<String>> handler) {
        setUpQueue(this::populatePositionReportQueue, handler);
    }

    @Override
    public void setUpRiskLimitUtilizationQueue(Handler<AsyncResult<String>> handler) {
        setUpQueue(this::populateRiskLimitUtilizationQueue, handler);
    }

    @Override
    public void drainAccountMarginQueue(Async async, Handler<AsyncResult<Void>> handler) {
        this.setUpDrain(connection -> this.drainConnection(connection, async, this.config.getListeners().getAccountMargin()), handler);
        async.awaitSuccess(30000);
    }

    private Future<ProtonConnection> drainConnection(ProtonConnection connection, Async async, String queueName) {
        connection.createReceiver(queueName)
                .setPrefetch(1000)
                .setAutoAccept(true)
                .handler((delivery, msg) -> async.countDown())
                .open();

        return Future.succeededFuture(connection);
    }

    private void setUpDrain(Function<ProtonConnection, Future<ProtonConnection>> drainFunction, Handler<AsyncResult<Void>> handler) {
        this.createAmqpConnection()
                .compose(drainFunction)
                .map((Void)null)
                .setHandler(handler);
    }

    private void setUpQueue(Function<ProtonConnection, Future<ProtonConnection>> populateFunction, Handler<AsyncResult<String>> handler) {
        this.createAmqpConnection()
                .compose(populateFunction)
                .map((String)null)
                .setHandler(handler);
    }

    private Future<ProtonConnection> createAmqpConnection() {
        Future<ProtonConnection> createAmqpConnectionFuture = Future.future();
        ProtonClient protonClient = ProtonClient.create(vertx);
        final String userName = this.config.getUsername();
        final String password = this.config.getPassword();
        protonClient.connect("localhost", this.tcpPort, userName, password, connectResult -> {
            if (connectResult.succeeded()) {
                connectResult.result().setContainer("mdh/marketDataLoaderIT").openHandler(openResult -> {
                    if (openResult.succeeded()) {
                        BrokerFillerCorrectData.protonConnection = openResult.result();
                        createAmqpConnectionFuture.complete(BrokerFillerCorrectData.protonConnection);
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
        final String queueName = this.config.getListeners().getAccountMargin();
        final Collection<Integer> ttsaveNumbers = IntStream.rangeClosed(1, 1)
                .boxed()
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, queueName, ttsaveNumbers, this::createAccountMarginGPBObjectList);
    }

    private Future<ProtonConnection> populateLiquiGroupMarginQueue(ProtonConnection protonConnection) {
        final String queueName = this.config.getListeners().getLiquiGroupMargin();
        final Collection<Integer> ttsaveNumbers = IntStream.rangeClosed(1, 1)
                .boxed()
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, queueName, ttsaveNumbers, this::createLiquiGroupMarginGPBObjectList);
    }

    private Future<ProtonConnection> populateLiquiGroupSplitMarginQueue(ProtonConnection protonConnection) {
        final String queueName = this.config.getListeners().getLiquiGroupSplitMargin();
        final Collection<Integer> ttsaveNumbers = IntStream.rangeClosed(1, 1)
                .boxed()
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, queueName, ttsaveNumbers, this::createLiquiGroupSplitMarginGPBObjectList);
    }

    private Future<ProtonConnection> populatePoolMarginQueue(ProtonConnection protonConnection) {
        final String queueName = this.config.getListeners().getPoolMargin();
        final Collection<Integer> ttsaveNumbers = IntStream.rangeClosed(1, 1)
                .boxed()
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, queueName, ttsaveNumbers, this::createPoolMarginGPBObjectList);
    }

    private Future<ProtonConnection> populatePositionReportQueue(ProtonConnection protonConnection) {
        final String queueName = this.config.getListeners().getPositionReport();
        final Collection<Integer> ttsaveNumbers = IntStream.rangeClosed(1, 1)
                .boxed()
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, queueName, ttsaveNumbers, this::createPositionReportGPBObjectList);
    }

    private Future<ProtonConnection> populateRiskLimitUtilizationQueue(ProtonConnection protonConnection) {
        final String queueName = this.config.getListeners().getRiskLimitUtilization();
        final Collection<Integer> ttsaveNumbers = IntStream.rangeClosed(1, 1)
                .boxed()
                .collect(Collectors.toList());
        return this.populateQueue(protonConnection, queueName, ttsaveNumbers, this::createRiskLimitUtilizationGPBObjectList);
    }

    protected Future<ProtonConnection> populateQueue(ProtonConnection protonConnection, String queueName, Collection<Integer> ttsaveNumbers, Function<Integer, Optional<ObjectList.GPBObjectList>> gpbBuilder) {
        Future<ProtonConnection> populateQueueFuture = Future.future();
        protonConnection.createSender(queueName).openHandler(openResult -> {
            if (openResult.succeeded()) {
                boolean allSent = true;
                ProtonSender sender = openResult.result();
                sender.setAutoSettle(true);
                for (Integer ttsaveNumber: ttsaveNumbers) {
                    Message message = Message.Factory.create();
                    Optional<ObjectList.GPBObjectList> gpbObjectList = gpbBuilder.apply(ttsaveNumber);
                    if (gpbObjectList.isPresent()) {
                        byte[] messageBytes = gpbObjectList.get().toByteArray();
                        message.setBody(new Data(new Binary(messageBytes)));
                        sender.send(message);
                    } else {
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

    private Optional<ObjectList.GPBObjectList> createAccountMarginGPBObjectList(int ttsaveNo) {
        final String folderName = "accountMargin";
        Function<JsonObject, ObjectList.GPBObject> creator = (json) -> {
            PrismaReports.AccountMargin data = DataHelper.createAccountMarginGPBFromJson(json);
            return ObjectList.GPBObject.newBuilder()
                    .setExtension(PrismaReports.accountMargin, data).build();
        };
        return this.createGPBFromJson(folderName, ttsaveNo, creator);
    }

    private Optional<ObjectList.GPBObjectList> createLiquiGroupMarginGPBObjectList(int ttsaveNo) {
        final String folderName = "liquiGroupMargin";
        Function<JsonObject, ObjectList.GPBObject> creator = (json) -> {
            PrismaReports.LiquiGroupMargin data = DataHelper.createLiquiGroupMarginGPBFromJson(json);
            return ObjectList.GPBObject.newBuilder()
                    .setExtension(PrismaReports.liquiGroupMargin, data).build();
        };
        return this.createGPBFromJson(folderName, ttsaveNo, creator);
    }

    private Optional<ObjectList.GPBObjectList> createLiquiGroupSplitMarginGPBObjectList(int ttsaveNo) {
        final String folderName = "liquiGroupSplitMargin";
        Function<JsonObject, ObjectList.GPBObject> creator = (json) -> {
            PrismaReports.LiquiGroupSplitMargin data = DataHelper.createLiquiGroupSplitMarginGPBFromJson(json);
            return ObjectList.GPBObject.newBuilder()
                    .setExtension(PrismaReports.liquiGroupSplitMargin, data).build();
        };
        return this.createGPBFromJson(folderName, ttsaveNo, creator);
    }

    private Optional<ObjectList.GPBObjectList> createPoolMarginGPBObjectList(int ttsaveNo) {
        final String folderName = "poolMargin";
        Function<JsonObject, ObjectList.GPBObject> creator = (json) -> {
            PrismaReports.PoolMargin data = DataHelper.createPoolMarginGPBFromJson(json);
            return ObjectList.GPBObject.newBuilder()
                    .setExtension(PrismaReports.poolMargin, data).build();
        };
        return this.createGPBFromJson(folderName, ttsaveNo, creator);
    }

    private Optional<ObjectList.GPBObjectList> createPositionReportGPBObjectList(int ttsaveNo) {
        final String folderName = "positionReport";
        Function<JsonObject, ObjectList.GPBObject> creator = (json) -> {
            PrismaReports.PositionReport data = DataHelper.createPositionReportGPBFromJson(json);
            return ObjectList.GPBObject.newBuilder()
                    .setExtension(PrismaReports.positionReport, data).build();
        };
        return this.createGPBFromJson(folderName, ttsaveNo, creator);
    }

    private Optional<ObjectList.GPBObjectList> createRiskLimitUtilizationGPBObjectList(int ttsaveNo) {
        final String folderName = "riskLimitUtilization";
        Function<JsonObject, ObjectList.GPBObject> creator = (json) -> {
            PrismaReports.RiskLimitUtilization data = DataHelper.createRiskLimitUtilizationGPBFromJson(json);
            return ObjectList.GPBObject.newBuilder()
                    .setExtension(PrismaReports.riskLimitUtilization, data).build();
        };
        return this.createGPBFromJson(folderName, ttsaveNo, creator);
    }

    protected Optional<ObjectList.GPBObjectList> createGPBFromJson(String folderName, int ttsaveNo, Function<JsonObject, ObjectList.GPBObject> creator) {
        ObjectList.GPBObjectList.Builder gpbObjectListBuilder = ObjectList.GPBObjectList.newBuilder();
        JsonObject lastRecord = new JsonObject();
        DataHelper.readTTSaveFile(folderName, ttsaveNo).forEach(json -> {
            ObjectList.GPBObject gpbObject = creator.apply(json);
            gpbObjectListBuilder.addItem(gpbObject);
            lastRecord.mergeIn(json);
        });
        if (lastRecord.isEmpty()) {
            return Optional.empty();
        }
        ObjectList.GPBHeader gpbHeader = ObjectList.GPBHeader.newBuilder()
                .setExtension(PrismaReports.prismaHeader, DataHelper.createPrismaHeaderFromJson(lastRecord)).build();
        gpbObjectListBuilder.setHeader(gpbHeader);
        return Optional.of(gpbObjectListBuilder.build());
    }
}
