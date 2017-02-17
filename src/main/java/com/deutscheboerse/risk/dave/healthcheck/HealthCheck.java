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
    }

    /**
     * Indicates whether all verticles are running properly.
     *
     * @return {@code true} if all verticles are up and running.
     */
    public boolean ready() {
        LOG.trace("Received readiness query");
        return this.getMainState();
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

    /**
     * Get global state of the {@link MainVerticle} components.
     *
     * @return {@code true} if {@link MainVerticle} is ready.
     */
    boolean getMainState() {
        LOG.trace("Received readiness query for {}", MAIN_KEY);
        return localMap.get(MAIN_KEY);
    }
}
