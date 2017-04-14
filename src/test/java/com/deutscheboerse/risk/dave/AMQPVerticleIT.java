package com.deutscheboerse.risk.dave;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.deutscheboerse.risk.dave.log.TestAppender;
import com.deutscheboerse.risk.dave.utils.*;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;

@RunWith(VertxUnitRunner.class)
public class AMQPVerticleIT {
    private final TestAppender testAppender = TestAppender.getAppender(AMQPVerticle.class);
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
    public void testConnectionSuccess(TestContext context) throws InterruptedException {
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getAmqpConfig());

        // Catch log messages generated by AccountMarginVerticle
        testAppender.start();
        vertx.deployVerticle(AccountMarginVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        testAppender.waitForMessageContains(Level.INFO, "AccountMarginVerticle connected to the broker");
        testAppender.stop();
    }

    @Test
    public void testConnectionFailure(TestContext context) throws InterruptedException {
        JsonObject config = TestConfig.getAmqpConfig();
        config.put("listeners", new JsonObject()
                .put("accountMargin", "nonexisting"));
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(config);

        testAppender.start();
        vertx.deployVerticle(AccountMarginVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        testAppender.waitForMessageContains(Level.ERROR, "AccountMarginVerticle failed to connect");
        testAppender.stop();
    }

    @Test
    public void testCreateReceiverFailure(TestContext context) throws InterruptedException {
        JsonObject config = TestConfig.getAmqpConfig();
        config.put("hostname", "nonexisting")
                .put("reconnectAttempts", 0);
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(config);

        // Catch log messages generated by AccountMarginVerticle
        testAppender.start();
        vertx.deployVerticle(AccountMarginVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        testAppender.waitForMessageContains(Level.ERROR, "AccountMarginVerticle failed to connect");
        testAppender.stop();
    }

    @Test
    public void testMissingGPBHeaderError(TestContext context) throws InterruptedException {
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getAmqpConfig());

        BrokerFiller brokerFiller = new BrokerFillerMissingHeader(this.vertx);
        brokerFiller.setUpAccountMarginQueue(context.asyncAssertSuccess());

        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        vertx.deployVerticle(AccountMarginVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        testAppender.waitForMessageContains(Level.ERROR, "Message header is missing for message - ignoring it");
        testAppender.stop();
        rootLogger.addAppender(stdout);
    }

    @Test
    public void testWrongGPBBodyError(TestContext context) throws InterruptedException {
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getAmqpConfig());

        BrokerFiller brokerFiller = new BrokerFillerWrongBody(this.vertx);
        brokerFiller.setUpAccountMarginQueue(context.asyncAssertSuccess());

        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        vertx.deployVerticle(AccountMarginVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        testAppender.waitForMessageContains(Level.ERROR, "Incoming message's body is not a 'data' type, skipping ... ");
        testAppender.stop();
        rootLogger.addAppender(stdout);
    }

    @Test
    public void testInvalidGPBError(TestContext context) throws InterruptedException {
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getAmqpConfig());

        BrokerFiller brokerFiller = new BrokerFillerInvalidProtocol(this.vertx);
        brokerFiller.setUpAccountMarginQueue(context.asyncAssertSuccess());

        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        vertx.deployVerticle(AccountMarginVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        testAppender.waitForMessageContains(Level.ERROR, "Unable to decode GPB message");
        testAppender.stop();
        rootLogger.addAppender(stdout);
    }
}