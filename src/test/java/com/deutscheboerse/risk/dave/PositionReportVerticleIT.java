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

import com.deutscheboerse.risk.dave.model.PositionReportModel;

@RunWith(VertxUnitRunner.class)
public class PositionReportVerticleIT extends BaseTest {
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
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(BaseTest.getBrokerConfig());
        // we expect 3596 messages to be received
        Async async = context.async(3596);

        // Setup persistence persistence
        CountdownPersistenceService persistenceService = new CountdownPersistenceService(vertx, async);
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        // Fill in the broker
        final BrokerFiller brokerFiller = new BrokerFiller(this.vertx);
        brokerFiller.setUpPositionReportQueue(context.asyncAssertSuccess());

        vertx.deployVerticle(PositionReportVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        async.awaitSuccess(30000);

        PositionReportModel expected = new PositionReportModel(new JsonObject()
                .put("snapshotID", 15)
                .put("businessDate", 20091215)
                .put("timestamp", 1487677414791L)
                .put("clearer", "ECAXX")
                .put("member", "ECAXX")
                .put("account", "A1")
                .put("liquidationGroup", "PFI02")
                .put("liquidationGroupSplit", "PFI02_HP5_T6-99999")
                .put("product", "OTC Portfolio")
                .put("callPut", "")
                .put("contractYear", 0)
                .put("contractMonth", 0)
                .put("expiryDay", 0)
                .put("exercisePrice", 0)
                .put("version", "")
                .put("flexContractSymbol", "")
                .put("netQuantityLs", 0)
                .put("netQuantityEa", 0)
                .put("clearingCurrency", "EUR")
                .put("mVar", -1038.9371665706567)
                .put("compVar", -1038.9371665706801)
                .put("compCorrelationBreak", 0)
                .put("compCompressionError", 0)
                .put("compLiquidityAddOn", 91236.25738021567)
                .put("compLongOptionCredit", 0)
                .put("productCurrency", "")
                .put("variationPremiumPayment", 0)
                .put("premiumMargin", 0)
                .put("normalizedDelta", 0)
                .put("normalizedGamma", 0)
                .put("normalizedVega", 0)
                .put("normalizedRho", 0)
                .put("normalizedTheta", 0)
                .put("underlying", "OTC Portfolio"));

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
        brokerFiller.setUpPositionReportQueue(context.asyncAssertSuccess());

        // Catch log messages generated by PositionReportVerticle
        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        vertx.deployVerticle(PositionReportVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        ILoggingEvent logMessage = testAppender.getLastMessage();
        testAppender.waitForMessageCount(3596);
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
        config.getJsonObject("listeners").put("positionReport", "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVERiskLimitUtilization");
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(config);

        // Fill in the incorrect queue PRISMA_TTSAVERiskLimitUtilization
        BrokerFiller brokerFiller = new BrokerFiller(this.vertx);
        brokerFiller.setUpRiskLimitUtilizationQueue(context.asyncAssertSuccess());

        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        vertx.deployVerticle(PositionReportVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        testAppender.waitForMessageCount(2);
        ILoggingEvent logMessage = testAppender.getLastMessage();
        testAppender.stop();
        rootLogger.addAppender(stdout);

        context.assertEquals(Level.ERROR, logMessage.getLevel());
        context.assertEquals("Unknown extension (should be position_report)", logMessage.getFormattedMessage());
    }
}
