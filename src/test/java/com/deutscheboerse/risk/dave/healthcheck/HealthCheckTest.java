package com.deutscheboerse.risk.dave.healthcheck;

import io.vertx.core.Vertx;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class HealthCheckTest {

    private final static String MAP_NAME = "healthCheck";
    private static final String ACCOUNT_MARGIN_KEY = "accountMarginReady";
    private static final String LIQUI_GROUP_MARGIN_KEY = "liquiGroupMarginReady";
    private static final String LIQUI_GROUP_SPLIT_MARGIN_KEY = "liquiGroupSplitMarginReady";
    private static final String POOL_MARGIN_KEY = "pooMarginReady";
    private static final String POSITION_REPORT_KEY = "positionReportReady";
    private static final String RISK_LIMIT_UTILIZATION_KEY = "riskLimitUtilizationReady";

    @Test
    public void testInitialization(TestContext context) {
        Vertx vertx = Vertx.vertx();

        HealthCheck healthCheck = new HealthCheck(vertx);
        LocalMap<String, Boolean> localMap = vertx.sharedData().getLocalMap(MAP_NAME);

        context.assertNotNull(localMap.get(ACCOUNT_MARGIN_KEY), "ACCOUNT_MARGIN readiness should not be null");
        context.assertFalse(localMap.get(ACCOUNT_MARGIN_KEY), "ACCOUNT_MARGIN readiness should be initialized to false");

        context.assertNotNull(localMap.get(LIQUI_GROUP_MARGIN_KEY), "LIQUI_GROUP_MARGIN readiness should not be null");
        context.assertFalse(localMap.get(LIQUI_GROUP_MARGIN_KEY), "LIQUI_GROUP_MARGIN readiness should be initialized to false");

        context.assertNotNull(localMap.get(LIQUI_GROUP_SPLIT_MARGIN_KEY), "LIQUI_GROUP_SPLIT_MARGIN readiness should not be null");
        context.assertFalse(localMap.get(LIQUI_GROUP_SPLIT_MARGIN_KEY), "LIQUI_GROUP_SPLIT_MARGIN readiness should be initialized to false");

        context.assertNotNull(localMap.get(POOL_MARGIN_KEY), "POOL_MARGIN readiness should not be null");
        context.assertFalse(localMap.get(POOL_MARGIN_KEY), "POOL_MARGIN readiness should be initialized to false");

        context.assertNotNull(localMap.get(POSITION_REPORT_KEY), "POSITION_REPORT readiness should not be null");
        context.assertFalse(localMap.get(POSITION_REPORT_KEY), "POSITION_REPORT readiness should be initialized to false");

        context.assertNotNull(localMap.get(RISK_LIMIT_UTILIZATION_KEY), "RISK_LIMIT_UTILIZATION readiness should not be null");
        context.assertFalse(localMap.get(RISK_LIMIT_UTILIZATION_KEY), "RISK_LIMIT_UTILIZATION readiness should be initialized to false");

        context.assertFalse(healthCheck.ready(), "Initial state of healthCheck should be false");

        vertx.close();
    }

    @Test
    public void testSomeVerticlesAreReady(TestContext context) {
        Vertx vertx = Vertx.vertx();

        HealthCheck healthCheck = new HealthCheck(vertx)
                .setAccountMarginState(true)
                .setLiquiGroupSplitMarginState(true)
                .setPoolMarginState(true)
                .setRiskLimitUtilizationState(true);

        context.assertFalse(healthCheck.ready(), "Only some verticles are ready, should return false");

        vertx.close();
    }

    @Test
    public void testAllVerticlesAreReady(TestContext context) {
        Vertx vertx = Vertx.vertx();

        HealthCheck healthCheck = new HealthCheck(vertx)
                .setAccountMarginState(true)
                .setLiquiGroupMarginState(true)
                .setLiquiGroupSplitMarginState(true)
                .setPoolMarginState(true)
                .setPositionReportState(true)
                .setRiskLimitUtilizationState(true);

        context.assertTrue(healthCheck.ready(), "All verticles are ready, the whole app should be ready");

        vertx.close();
    }
}
