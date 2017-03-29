package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.persistence.CountdownPersistenceService;
import com.deutscheboerse.risk.dave.persistence.SuccessPersistenceService;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import com.deutscheboerse.risk.dave.utils.BrokerFiller;
import com.deutscheboerse.risk.dave.utils.BrokerFillerCorrectData;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import com.deutscheboerse.risk.dave.utils.TestConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
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

import java.io.IOException;

@RunWith(VertxUnitRunner.class)
public class MainVerticleIT {
    private Vertx vertx;
    private static PersistenceService countdownService;
    private static int ACCOUNT_MARGIN_COUNT = DataHelper.getJsonObjectCount("accountMargin", 1);
    private static int LIQUI_GROUP_MARGIN_COUNT = DataHelper.getJsonObjectCount("liquiGroupMargin", 1);
    private static int LIQUI_GROUP_SPLIT_MARGIN_COUNT = DataHelper.getJsonObjectCount("liquiGroupSplitMargin", 1);
    private static int POOL_MARGIN_COUNT = DataHelper.getJsonObjectCount("poolMargin", 1);
    private static int POSITION_REPORT_COUNT = DataHelper.getJsonObjectCount("positionReport", 1);
    private static int RISK_LIMIT_UTILIZATION_COUNT = DataHelper.getJsonObjectCount("riskLimitUtilization", 1);

    @Before
    public void setUp() {
        this.vertx = Vertx.vertx();
    }

    private DeploymentOptions createDeploymentOptions(Class<? extends AbstractModule> binder) {
        JsonObject config = TestConfig.getGlobalConfig();
        config.put("guice_binder", binder.getName());
        return new DeploymentOptions().setConfig(config);
    }

    @Test
    public void testFullChain(TestContext context) throws IOException, InterruptedException {
        Async mainVerticleAsync = context.async();
        Async totalMsgCountAsync = context.async(
                ACCOUNT_MARGIN_COUNT
                   + LIQUI_GROUP_MARGIN_COUNT
                   + LIQUI_GROUP_SPLIT_MARGIN_COUNT
                   + POOL_MARGIN_COUNT
                   + POSITION_REPORT_COUNT
                   + RISK_LIMIT_UTILIZATION_COUNT);

        countdownService = new CountdownPersistenceService(totalMsgCountAsync);
        DeploymentOptions options = createDeploymentOptions(CountdownBinder.class);
        this.vertx.deployVerticle(MainVerticle.class.getName(), options, ar -> {
            if (ar.succeeded()) {
                mainVerticleAsync.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        mainVerticleAsync.awaitSuccess(30000);
        final BrokerFiller brokerFiller = new BrokerFillerCorrectData(this.vertx);
        brokerFiller.setUpAllQueues(context.asyncAssertSuccess());

        totalMsgCountAsync.awaitSuccess(30000);
    }

    @Test
    public void testFailedDeploymentWrongConfig(TestContext context) {
        Async mainVerticleAsync = context.async();
        DeploymentOptions options = createDeploymentOptions(SuccessBinder.class);
        System.setProperty("dave.configurationFile", "nonexisting");
        this.vertx.deployVerticle(MainVerticle.class.getName(), options, ar -> {
            System.clearProperty("dave.configurationFile");
            if (ar.succeeded()) {
                context.fail(ar.cause());
            } else {
                mainVerticleAsync.complete();
            }
        });
    }

    @Test
    public void testFailedDeployment(TestContext context) {
        DeploymentOptions options = createDeploymentOptions(SuccessBinder.class);
        options.getConfig().getJsonObject("healthCheck", new JsonObject()).put("port", -1);
        this.vertx.deployVerticle(MainVerticle.class.getName(), options, context.asyncAssertFailure());
    }

    @After
    public void cleanup(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }


    public static class SuccessBinder extends AbstractModule {
        @Override
        protected void configure() {
            bind(PersistenceService.class).to(SuccessPersistenceService.class).in(Singleton.class);
        }
    }

    public static class CountdownBinder extends AbstractModule {
        @Override
        protected void configure() {
            bind(PersistenceService.class).toInstance(countdownService);
        }
    }
}
