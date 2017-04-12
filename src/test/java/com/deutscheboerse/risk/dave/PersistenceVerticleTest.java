package com.deutscheboerse.risk.dave;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.deutscheboerse.risk.dave.log.TestAppender;
import com.deutscheboerse.risk.dave.persistence.InitPersistenceService;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import com.deutscheboerse.risk.dave.utils.TestConfig;
import com.google.inject.AbstractModule;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;

@RunWith(VertxUnitRunner.class)
public class PersistenceVerticleTest {
    private final TestAppender testAppender = TestAppender.getAppender(PersistenceVerticle.class);
    private final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private Vertx vertx;
    private static InitPersistenceService persistenceService;

    @Before
    public void setUp() {
        this.vertx = Vertx.vertx();
        rootLogger.addAppender(testAppender);
    }

    @Test
    public void checkPersistenceServiceDeployment(TestContext context) throws InterruptedException {
        JsonObject config = TestConfig.getStorageConfig();
        config.put("guice_binder", PersistenceBinder.class.getName());
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        testAppender.start();
        this.vertx.deployVerticle("java-guice:" + PersistenceVerticle.class.getName(), options, context.asyncAssertSuccess());
        testAppender.waitForMessageContains(Level.INFO, "Persistence verticle started");
        testAppender.stop();
    }

    @Test
    public void checkPersistenceServiceInitialized(TestContext context) {
        PersistenceVerticleTest.persistenceService = new InitPersistenceService(true);
        JsonObject config = TestConfig.getStorageConfig();
        config.put("guice_binder", TestBinder.class.getName());
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        Async async = context.async();
        this.vertx.deployVerticle("java-guice:" + PersistenceVerticle.class.getName(), options, ar -> {
            if (ar.succeeded()) {
                async.complete();
            } else {
                context.fail((ar.cause()));
            }
        });
        async.awaitSuccess(10000);
        context.assertTrue(persistenceService.isInitialized());
    }

    @Test
    public void checkPersistenceServiceNotInitialized(TestContext context) {
        PersistenceVerticleTest.persistenceService = new InitPersistenceService(false);
        JsonObject config = TestConfig.getStorageConfig();
        config.put("guice_binder", TestBinder.class.getName());
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        Async async = context.async();
        this.vertx.deployVerticle("java-guice:" + PersistenceVerticle.class.getName(), options, ar -> {
            if (ar.succeeded()) {
                context.fail((ar.cause()));
            } else {
                async.complete();
            }
        });
        async.awaitSuccess(10000);
        context.assertFalse(persistenceService.isInitialized());
    }

    @After
    public void tearDown(TestContext context) {
        rootLogger.detachAppender(testAppender);
        this.vertx.close(context.asyncAssertSuccess());
    }

    public static class TestBinder extends AbstractModule {

        @Override
        protected void configure() {
            bind(PersistenceService.class).toInstance(persistenceService);
        }
    }
}
