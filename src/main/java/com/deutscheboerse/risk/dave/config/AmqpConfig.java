package com.deutscheboerse.risk.dave.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AmqpConfig {
    private static final String DEFAULT_USERNAME = "admin";
    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final int DEFAULT_PORT = 5672;
    private static final int DEFAULT_RECONNECT_ATTEMPTS = -1;
    private static final int DEFAULT_RECONNECT_TIMEOUT = 60000;
    private final String username;
    private final String password;
    private final String hostname;
    private final int port;
    private final int reconnectAttempts;
    private final int reconnectTimeout;
    private final ListenersConfig listeners;
    private final CircuitBreakerConfig circuitBreaker;

    @JsonCreator
    public AmqpConfig(@JsonProperty("username") String username,
                      @JsonProperty("password") String password,
                      @JsonProperty("hostname") String hostname,
                      @JsonProperty("port") Integer port,
                      @JsonProperty("reconnectAttempts") Integer reconnectAttempts,
                      @JsonProperty("reconnectTimeout") Integer reconnectTimeout,
                      @JsonProperty("listeners") ListenersConfig listeners,
                      @JsonProperty("circuitBreaker") CircuitBreakerConfig circuitBreaker) {
        this.username = username == null ? DEFAULT_USERNAME : username;
        this.password = password;
        this.hostname = hostname == null ? DEFAULT_HOSTNAME : hostname;
        this.port = port == null ? DEFAULT_PORT : port;
        this.reconnectAttempts = reconnectAttempts == null ? DEFAULT_RECONNECT_ATTEMPTS : reconnectAttempts;
        this.reconnectTimeout = reconnectTimeout == null ? DEFAULT_RECONNECT_TIMEOUT : reconnectTimeout;
        this.listeners = listeners;
        this.circuitBreaker = circuitBreaker;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public int getReconnectAttempts() {
        return reconnectAttempts;
    }

    public int getReconnectTimeout() {
        return reconnectTimeout;
    }

    public ListenersConfig getListeners() {
        return listeners;
    }

    public CircuitBreakerConfig getCircuitBreaker() {
        return circuitBreaker;
    }

    public static class ListenersConfig {
        private static final String DEFAULT_ACCOUNT_MARGIN = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEAccountMargin";
        private static final String DEFAULT_LIQUI_GROUP_MARGIN = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupMargin";
        private static final String DEFAULT_LIQUI_GROUP_SPLIT_MARGIN = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupSplitMargin";
        private static final String DEFAULT_POSITION_REPORT = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEPositionReport";
        private static final String DEFAULT_POOL_MARGIN = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEPoolMargin";
        private static final String DEFAULT_RISK_LIMIT_UTILIZATION = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVERiskLimitUtilization";
        private final String accountMargin;
        private final String liquiGroupMargin;
        private final String liquiGroupSplitMargin;
        private final String positionReport;
        private final String poolMargin;
        private final String riskLimitUtilization;

        @JsonCreator
        public ListenersConfig(@JsonProperty("accountMargin") String accountMargin,
                               @JsonProperty("liquiGroupMargin") String liquiGroupMargin,
                               @JsonProperty("liquiGroupSplitMargin") String liquiGroupSplitMargin,
                               @JsonProperty("positionReport") String positionReport,
                               @JsonProperty("poolMargin") String poolMargin,
                               @JsonProperty("riskLimitUtilization") String riskLimitUtilization) {
            this.accountMargin = accountMargin == null ? DEFAULT_ACCOUNT_MARGIN : accountMargin;
            this.liquiGroupMargin = liquiGroupMargin == null ? DEFAULT_LIQUI_GROUP_MARGIN : liquiGroupMargin;
            this.liquiGroupSplitMargin = liquiGroupSplitMargin == null ? DEFAULT_LIQUI_GROUP_SPLIT_MARGIN : liquiGroupSplitMargin;
            this.positionReport = positionReport == null ? DEFAULT_POSITION_REPORT : positionReport;
            this.poolMargin = poolMargin == null ? DEFAULT_POOL_MARGIN : poolMargin;
            this.riskLimitUtilization = riskLimitUtilization == null ? DEFAULT_RISK_LIMIT_UTILIZATION : riskLimitUtilization;

        }

        public String getAccountMargin() {
            return accountMargin;
        }

        public String getLiquiGroupMargin() {
            return liquiGroupMargin;
        }

        public String getLiquiGroupSplitMargin() {
            return liquiGroupSplitMargin;
        }

        public String getPositionReport() {
            return positionReport;
        }

        public String getPoolMargin() {
            return poolMargin;
        }

        public String getRiskLimitUtilization() {
            return riskLimitUtilization;
        }
    }

    public static class CircuitBreakerConfig {
        private static final int DEFAULT_MAX_FAILURES = 5;
        private static final int DEFAULT_TIMEOUT = 10000;
        private static final int DEFAULT_RESET_TIMEOUT = 30000;
        private final int maxFailures;
        private final int timeout;
        private final int resetTimeout;

        @JsonCreator
        public CircuitBreakerConfig(@JsonProperty("maxFailures") Integer maxFailures,
                                    @JsonProperty("timeout") Integer timeout,
                                    @JsonProperty("resetTimeout") Integer resetTimeout) {
            this.maxFailures = maxFailures == null ? DEFAULT_MAX_FAILURES : maxFailures;
            this.timeout = timeout == null ? DEFAULT_TIMEOUT : timeout;
            this.resetTimeout = resetTimeout == null ? DEFAULT_RESET_TIMEOUT : resetTimeout;

        }

        public int getMaxFailures() {
            return maxFailures;
        }

        public int getTimeout() {
            return timeout;
        }

        public int getResetTimeout() {
            return resetTimeout;
        }
    }
}
