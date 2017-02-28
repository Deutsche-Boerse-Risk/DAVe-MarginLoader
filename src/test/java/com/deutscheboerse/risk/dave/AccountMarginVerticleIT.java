package com.deutscheboerse.risk.dave;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.deutscheboerse.risk.dave.log.TestAppender;
import com.deutscheboerse.risk.dave.persistence.CountdownPersistenceService;
import com.deutscheboerse.risk.dave.persistence.ErrorPersistenceService;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
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

import com.deutscheboerse.risk.dave.model.AccountMarginModel;

@RunWith(VertxUnitRunner.class)
public class AccountMarginVerticleIT extends BaseTest {
    private final TestAppender testAppender = TestAppender.getAppender(AccountMarginVerticle.class);
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
    public void testAccountMarginVerticle(TestContext context) throws InterruptedException {
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(BaseTest.getBrokerConfig());
        // we expect 1704 messages to be received
        Async async = context.async(1704);

        // Setup persistence persistence
        CountdownPersistenceService persistenceService = new CountdownPersistenceService(vertx, async);
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        // Fill in the broker
        BrokerFiller brokerFiller = new BrokerFiller(vertx);
        brokerFiller.setUpAccountMarginQueue(context.asyncAssertSuccess());

        vertx.deployVerticle(AccountMarginVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        async.awaitSuccess(30000);

        // verify the content of the last message
        AccountMarginModel expected = new AccountMarginModel(new JsonObject()
                .put("snapshotID", 10)
                .put("businessDate", 20091215)
                .put("timestamp", 1487172422430L)
                .put("clearer", "SFUCC")
                .put("member", "SFUFR")
                .put("account", "A5")
                .put("marginCurrency", "EUR")
                .put("clearingCurrency", "EUR")
                .put("pool", "default")
                .put("marginReqInMarginCurr", 5.035485884371926E7)
                .put("marginReqInCrlCurr", 5.035485884371926E7)
                .put("unadjustedMarginRequirement", 5.035485884371926E7)
                .put("variationPremiumPayment", 0.0));
        context.assertEquals(expected, persistenceService.getLastMessage());

        ProxyHelper.unregisterService(serviceMessageConsumer);
    }

    @Test
    public void testMessageStoreError(TestContext context) throws InterruptedException {
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(BaseTest.getBrokerConfig());
        // Setup persistence persistence
        ErrorPersistenceService persistenceService = new ErrorPersistenceService(vertx);
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        // Fill in the broker
        BrokerFiller brokerFiller = new BrokerFiller(vertx);
        brokerFiller.setUpAccountMarginQueue(context.asyncAssertSuccess());

        // Catch log messages generated by AccountMarginVerticle
        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        vertx.deployVerticle(AccountMarginVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        testAppender.waitForMessageCount(1704);
        ILoggingEvent logMessage = testAppender.getLastMessage();
        testAppender.stop();
        rootLogger.addAppender(stdout);

        context.assertEquals(Level.ERROR, logMessage.getLevel());
        context.assertTrue(logMessage.getFormattedMessage().contains("Unable to store message"));

        ProxyHelper.unregisterService(serviceMessageConsumer);
    }

    @Test
    public void testUnknownGPBExtensionError(TestContext context) throws InterruptedException {
        // Setup account margin to listen on incorrect queue
        JsonObject config = BaseTest.getBrokerConfig();
        config.getJsonObject("listeners").put("accountMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupMargin");
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(config);

        // Fill in the incorrect queue PRISMA_TTSAVELiquiGroupMargin
        BrokerFiller brokerFiller = new BrokerFiller(this.vertx);
        brokerFiller.setUpLiquiGroupMarginQueue(context.asyncAssertSuccess());

        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        vertx.deployVerticle(AccountMarginVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        testAppender.waitForMessageCount(2171);
        ILoggingEvent logMessage = testAppender.getLastMessage();
        testAppender.stop();
        rootLogger.addAppender(stdout);

        context.assertEquals(Level.ERROR, logMessage.getLevel());
        context.assertEquals("Unknown extension (should be account_margin)", logMessage.getFormattedMessage());
    }
}