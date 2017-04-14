package com.deutscheboerse.risk.dave;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.deutscheboerse.risk.dave.log.TestAppender;
import com.deutscheboerse.risk.dave.model.LiquiGroupMarginModel;
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
public class LiquiGroupMarginVerticleIT {
    private final TestAppender testAppender = TestAppender.getAppender(LiquiGroupMarginVerticle.class);
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
    public void testLiquiGroupMarginVerticle(TestContext context) throws InterruptedException {
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getAmqpConfig());
        // we expect 2171 messages to be received
        int msgCount = DataHelper.getJsonObjectCount("liquiGroupMargin", 1);
        Async async = context.async(msgCount);

        // Setup persistence persistence
        CountdownPersistenceService persistenceService = new CountdownPersistenceService(async);
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        // Fill in the broker
        final BrokerFiller brokerFiller = new BrokerFillerCorrectData(this.vertx);
        brokerFiller.setUpLiquiGroupMarginQueue(context.asyncAssertSuccess());

        vertx.deployVerticle(LiquiGroupMarginVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        async.awaitSuccess(30000);

        // verify the content of the last message
        JsonObject jsonData = DataHelper.getLastJsonFromFile("liquiGroupMargin", 1).orElse(new JsonObject());
        LiquiGroupMarginModel expected = new LiquiGroupMarginModel(jsonData);
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
        brokerFiller.setUpLiquiGroupMarginQueue(context.asyncAssertSuccess());

        // Catch log messages generated by LiquiGroupMarginVerticle
        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        vertx.deployVerticle(LiquiGroupMarginVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        int msgCount = DataHelper.getJsonObjectCount("liquiGroupMargin", 1);
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
        config.getJsonObject("listeners").put("liquiGroupMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupSplitMargin");
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(config);

        // Fill in the incorrect queue PRISMA_TTSAVELiquiGroupSplitMargin
        BrokerFiller brokerFiller = new BrokerFillerCorrectData(this.vertx);
        brokerFiller.setUpLiquiGroupSplitMarginQueue(context.asyncAssertSuccess());

        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        vertx.deployVerticle(LiquiGroupMarginVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        int msgCount = DataHelper.getJsonObjectCount("liquiGroupSplitMargin", 1);
        testAppender.waitForMessageCount(Level.ERROR, msgCount);
        testAppender.waitForMessageContains(Level.ERROR, "Unknown extension (should be liqui_group_margin)");
        testAppender.stop();
        rootLogger.addAppender(stdout);
    }

    @Test
    public void testInvalidGPBError(TestContext context) throws InterruptedException {
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getAmqpConfig());

        BrokerFiller brokerFiller = new BrokerFillerMissingField(this.vertx);
        brokerFiller.setUpLiquiGroupMarginQueue(context.asyncAssertSuccess());

        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        vertx.deployVerticle(LiquiGroupMarginVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        int msgCount = DataHelper.getJsonObjectCount("liquiGroupMargin", 1);
        testAppender.waitForMessageCount(Level.ERROR, msgCount);
        testAppender.waitForMessageContains(Level.ERROR, "Unable to create Liqui Group Margin Model from GPB data");
        testAppender.stop();
        rootLogger.addAppender(stdout);
    }

}
