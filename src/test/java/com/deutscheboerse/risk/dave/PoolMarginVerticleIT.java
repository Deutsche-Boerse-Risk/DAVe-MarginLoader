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

@RunWith(VertxUnitRunner.class)
public class PoolMarginVerticleIT  extends BaseTest {
    private final TestAppender testAppender = TestAppender.getAppender(PoolMarginVerticle.class);
    private final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private Vertx vertx;

    @Before
    public void setUp(TestContext context) {
        this.vertx = Vertx.vertx();
        rootLogger.addAppender(testAppender);
    }

    @After
    public void cleanup(TestContext context) {
        this.vertx.close(context.asyncAssertSuccess());
        rootLogger.detachAppender(testAppender);
    }

    @Test
    public void testPoolMarginVerticle(TestContext context) throws InterruptedException {
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(BaseTest.getBrokerConfig());
        // we expect 270 messages to be received
        Async async = context.async(270);

        // Setup persistence persistence
        CountdownPersistenceService persistenceService = new CountdownPersistenceService(vertx, async);
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        // Fill in the broker
        final BrokerFiller brokerFiller = new BrokerFiller(this.vertx);
        brokerFiller.setUpPoolMarginQueue(context.asyncAssertSuccess());

        vertx.deployVerticle(PoolMarginVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        async.awaitSuccess(30000);

        JsonObject expected = new JsonObject()
                .put("snapshotID", 10)
                .put("businessDate", 20091215)
                .put("timestamp", new JsonObject().put("$date", "2017-02-15T15:27:02.43Z"))
                .put("clearer", "CBKFR")
                .put("pool", "default")
                .put("marginCurrency", "CHF")
                .put("clrRptCurrency", "EUR")
                .put("requiredMargin", 0.0)
                .put("cashCollateralAmount", 920294764.124)
                .put("adjustedSecurities", 0.0)
                .put("adjustedGuarantee", 0.0)
                .put("overUnderInMarginCurr", 920294764.124)
                .put("overUnderInClrRptCurr", 688690802.130428)
                .put("variPremInMarginCurr", 920294764.124)
                .put("adjustedExchangeRate", 0.748337194753)
                .put("poolOwner", "CBKFR");
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
        final BrokerFiller brokerFiller = new BrokerFiller(this.vertx);
        brokerFiller.setUpPoolMarginQueue(context.asyncAssertSuccess());

        // Catch log messages generated by PoolMarginVerticle
        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        vertx.deployVerticle(PoolMarginVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        ILoggingEvent logMessage = testAppender.getLastMessage();
        testAppender.waitForMessageCount(270);
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
        config.getJsonObject("listeners").put("poolMargin", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEPositionReport");
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(config);

        // Fill in the incorrect queue PRISMA_TTSAVEPositionReport
        BrokerFiller brokerFiller = new BrokerFiller(this.vertx);
        brokerFiller.setUpPositionReportQueue(context.asyncAssertSuccess());

        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        vertx.deployVerticle(PoolMarginVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        testAppender.waitForMessageCount(3596);
        ILoggingEvent logMessage = testAppender.getLastMessage();
        testAppender.stop();
        rootLogger.addAppender(stdout);

        context.assertEquals(Level.ERROR, logMessage.getLevel());
        context.assertEquals("Unknown extension (should be pool_margin)", logMessage.getFormattedMessage());
    }
}
