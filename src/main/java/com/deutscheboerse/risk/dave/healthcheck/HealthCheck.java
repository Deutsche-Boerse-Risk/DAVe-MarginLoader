package com.deutscheboerse.risk.dave.healthcheck;

import com.deutscheboerse.risk.dave.MainVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.LocalMap;

/**
 * Holds a global state of all verticles which are vital for proper
 * run of DAVE-MarginLoader.
 * <p>
 * If all components are up and running the {@link HealthCheck#ready()}
 * method returns {@code true}.
 */
public class HealthCheck {
    private static final Logger LOG = LoggerFactory.getLogger(HealthCheck.class);

    private static final String MAP_NAME = "healthCheck";
    private static final String MAIN_KEY = "mainReady";
    private static final String ACCOUNT_MARGIN_KEY = "accountMarginReady";
    private static final String LIQUI_GROUP_MARGIN_KEY = "liquiGroupMarginReady";
    private static final String LIQUI_GROUP_SPLIT_MARGIN_KEY = "liquiGroupSplitMarginReady";
    private static final String POOL_MARGIN_KEY = "pooMarginReady";
    private static final String POSITION_REPORT_KEY = "positionReportReady";
    private static final String RISK_LIMIT_UTILIZATION_KEY = "riskLimitUtilizationReady";

    private LocalMap<String, Boolean> localMap;

    /**
     * Create a new instance.
     *
     * @param vertx All instances of {@code HealthCheck} created with identical
     *              {@code vertx} parameter share the same local map.
     */
    public HealthCheck(Vertx vertx) {
        LOG.trace("Constructing {} object", HealthCheck.class.getCanonicalName());
        localMap = vertx.sharedData().getLocalMap(MAP_NAME);
        localMap.putIfAbsent(MAIN_KEY, false);
        localMap.putIfAbsent(ACCOUNT_MARGIN_KEY, false);
        localMap.putIfAbsent(LIQUI_GROUP_MARGIN_KEY, false);
        localMap.putIfAbsent(LIQUI_GROUP_SPLIT_MARGIN_KEY, false);
        localMap.putIfAbsent(POOL_MARGIN_KEY, false);
        localMap.putIfAbsent(POSITION_REPORT_KEY, false);
        localMap.putIfAbsent(RISK_LIMIT_UTILIZATION_KEY, false);

    }

    /**
     * Indicates whether all verticles are running properly.
     *
     * @return {@code true} if all verticles are up and running.
     */
    public boolean ready() {
        LOG.trace("Received readiness query");
        // Return true only if all the values are true (the map
        // does not contain any single false)
        return !localMap.values().contains(false);
    }

    /**
     * Set global state of the {@link MainVerticle} component.
     *
     * @param state indicating health of {@link MainVerticle}.
     * @return a reference to this, so the API can be used fluently.
     */
    public HealthCheck setMainState(boolean state) {
        LOG.info("Setting {} readiness to {}", MAIN_KEY, state);
        localMap.put(MAIN_KEY, state);
        return this;
    }

    public HealthCheck setAccountMarginState(boolean state) {
        LOG.info("Setting {} readiness to {}", ACCOUNT_MARGIN_KEY, state);
        localMap.put(ACCOUNT_MARGIN_KEY, state);
        return this;
    }

    public HealthCheck setLiquiGroupMarginState(boolean state) {
        LOG.info("Setting {} readiness to {}", LIQUI_GROUP_MARGIN_KEY, state);
        localMap.put(LIQUI_GROUP_MARGIN_KEY, state);
        return this;
    }

    public HealthCheck setLiquiGroupSplitMarginState(boolean state) {
        LOG.info("Setting {} readiness to {}", LIQUI_GROUP_SPLIT_MARGIN_KEY, state);
        localMap.put(LIQUI_GROUP_SPLIT_MARGIN_KEY, state);
        return this;
    }

    public HealthCheck setPoolMarginState(boolean state) {
        LOG.info("Setting {} readiness to {}", POOL_MARGIN_KEY, state);
        localMap.put(POOL_MARGIN_KEY, state);
        return this;
    }

    public HealthCheck setPositionReportState(boolean state) {
        LOG.info("Setting {} readiness to {}", POSITION_REPORT_KEY, state);
        localMap.put(POSITION_REPORT_KEY, state);
        return this;
    }

    public HealthCheck setRiskLimitUtilizationState(boolean state) {
        LOG.info("Setting {} readiness to {}", RISK_LIMIT_UTILIZATION_KEY, state);
        localMap.put(RISK_LIMIT_UTILIZATION_KEY, state);
        return this;
    }

    /**
     * Get global state of the {@link MainVerticle} components.
     *
     * @return {@code true} if {@link MainVerticle} is ready.
     */
    boolean getMainState() {
        LOG.trace("Received readiness query for {}", MAIN_KEY);
        return localMap.get(MAIN_KEY);
    }

    boolean getAccountMarginState() {
        LOG.trace("Received readiness query for {}", ACCOUNT_MARGIN_KEY);
        return localMap.get(ACCOUNT_MARGIN_KEY);
    }

    boolean getLiquiGroupMarginState() {
        LOG.trace("Received readiness query for {}", LIQUI_GROUP_MARGIN_KEY);
        return localMap.get(LIQUI_GROUP_MARGIN_KEY);
    }

    boolean getLiquiGroupSplitMarginState() {
        LOG.trace("Received readiness query for {}", LIQUI_GROUP_SPLIT_MARGIN_KEY);
        return localMap.get(LIQUI_GROUP_SPLIT_MARGIN_KEY);
    }

    boolean getPooMarginState() {
        LOG.trace("Received readiness query for {}", POOL_MARGIN_KEY);
        return localMap.get(POOL_MARGIN_KEY);
    }

    boolean getPositionReportState() {
        LOG.trace("Received readiness query for {}", POSITION_REPORT_KEY);
        return localMap.get(POSITION_REPORT_KEY);
    }

    boolean getRiskLimitUtilizationState() {
        LOG.trace("Received readiness query for {}", RISK_LIMIT_UTILIZATION_KEY);
        return localMap.get(RISK_LIMIT_UTILIZATION_KEY);
    }
}
