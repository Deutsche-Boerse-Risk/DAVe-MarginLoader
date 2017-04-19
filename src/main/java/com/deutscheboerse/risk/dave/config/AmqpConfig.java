package com.deutscheboerse.risk.dave.config;

public class AmqpConfig {
    private String username = "admin";
    private String password = null;
    private String hostname = "localhost";
    private int port = 5672;
    private int reconnectAttempts = -1;
    private int reconnectTimeout = 60000;
    private ListenersConfig listeners = new ListenersConfig();

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

    public class ListenersConfig {
        private String accountMargin = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEAccountMargin";
        private String liquiGroupMargin = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupMargin";
        private String liquiGroupSplitMargin = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupSplitMargin";
        private String positionReport = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEPositionReport";
        private String poolMargin = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEPoolMargin";
        private String riskLimitUtilization = "broadcast.PRISMA_BRIDGE.PRISMA_TTSAVERiskLimitUtilization";

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
}
