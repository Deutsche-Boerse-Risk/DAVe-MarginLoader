package com.deutscheboerse.risk.dave;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.deutscheboerse.risk.dave.log.TestAppender;
import com.deutscheboerse.risk.dave.model.PositionReportModel;
import com.deutscheboerse.risk.dave.persistence.CountdownPersistenceService;
import com.deutscheboerse.risk.dave.persistence.ErrorPersistenceService;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import com.deutscheboerse.risk.dave.utils.*;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.serviceproxy.ProxyHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;

@RunWith(VertxUnitRunner.class)
public class PositionReportVerticleIT {
    private final TestAppender testAppender = TestAppender.getAppender(PositionReportVerticle.class);
    private final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private Vertx vertx;

    @Before
    public void setUp() {
        this.vertx = Vertx.vertx();
        rootLogger.addAppender(testAppender);
    }

    @After
    public void cleanup(TestContext context) {
        this.vertx.close(context.asyncAssertSuccess());
        rootLogger.detachAppender(testAppender);
    }

    @Test
    public void testPositionReportVerticle(TestContext context) throws InterruptedException {
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getAmqpConfig());
        // we expect 3596 messages to be received
        int msgCount = DataHelper.getJsonObjectCount("positionReport", 1);
        Async async = context.async(msgCount);

        // Setup persistence persistence
        CountdownPersistenceService persistenceService = new CountdownPersistenceService(async);
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        // Fill in the broker
        final BrokerFiller brokerFiller = new BrokerFillerCorrectData(this.vertx);
        brokerFiller.setUpPositionReportQueue(context.asyncAssertSuccess());

        vertx.deployVerticle(PositionReportVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        async.awaitSuccess(30000);

        JsonObject jsonData = DataHelper.getLastJsonFromFile("positionReport", 1).orElse(new JsonObject());
        PositionReportModel expected = new PositionReportModel(jsonData);
        context.assertEquals(expected, persistenceService.getLastMessage());

        ProxyHelper.unregisterService(serviceMessageConsumer);
    }

    @Test
    public void testMessageStoreError(TestContext context) throws InterruptedException {
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getAmqpConfig());
        // Setup persistence persistence
        ErrorPersistenceService persistenceService = new ErrorPersistenceService();
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        // Fill in the broker
        final BrokerFiller brokerFiller = new BrokerFillerCorrectData(this.vertx);
        brokerFiller.setUpPositionReportQueue(context.asyncAssertSuccess());

        // Catch log messages generated by PositionReportVerticle
        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        vertx.deployVerticle(PositionReportVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        int msgCount = DataHelper.getJsonObjectCount("positionReport", 1);
        testAppender.waitForMessageCount(Level.ERROR, msgCount);
        testAppender.waitForMessageContains(Level.ERROR, "Unable to store message");
        testAppender.stop();
        rootLogger.addAppender(stdout);

        ProxyHelper.unregisterService(serviceMessageConsumer);
    }

    @Test
    public void testUnknownGPBExtensionError(TestContext context) throws InterruptedException {
        // Setup account margin to listen on incorrect queue
        JsonObject config = TestConfig.getAmqpConfig();
        config.getJsonObject("listeners").put("positionReport", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVERiskLimitUtilization");
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(config);

        // Fill in the incorrect queue PRISMA_TTSAVERiskLimitUtilization
        BrokerFiller brokerFiller = new BrokerFillerCorrectData(this.vertx);
        brokerFiller.setUpRiskLimitUtilizationQueue(context.asyncAssertSuccess());

        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        vertx.deployVerticle(PositionReportVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        int msgCount = DataHelper.getJsonObjectCount("riskLimitUtilization", 1);
        testAppender.waitForMessageCount(Level.ERROR, msgCount);
        testAppender.waitForMessageContains(Level.ERROR, "Unknown extension (should be position_report)");
        testAppender.stop();
        rootLogger.addAppender(stdout);
    }

    @Test
    public void testInvalidGPBError(TestContext context) throws InterruptedException {
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getAmqpConfig());

        BrokerFiller brokerFiller = new BrokerFillerMissingField(this.vertx);
        brokerFiller.setUpPositionReportQueue(context.asyncAssertSuccess());

        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        vertx.deployVerticle(PositionReportVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        int msgCount = DataHelper.getJsonObjectCount("positionReport", 1);
        testAppender.waitForMessageCount(Level.ERROR, msgCount);
        testAppender.waitForMessageContains(Level.ERROR, "Unable to create Position Report Model from GPB data");
        testAppender.stop();
        rootLogger.addAppender(stdout);
    }

}
