package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

public class RiskLimitUtilizationModel extends AbstractModel {
    public static final String MONGO_HISTORY_COLLECTION = "RiskLimitUtilization";
    public static final String MONGO_LATEST_COLLECTION = "RiskLimitUtilization.latest";

    public RiskLimitUtilizationModel() {
        super();
    }

    public RiskLimitUtilizationModel(PrismaReports.PrismaHeader header, PrismaReports.RiskLimitUtilization data) {
        super(header);

        verify(data);

        put("clearer", data.getClearer());
        put("member", data.getMember());
        put("maintainer", data.getMaintainer());
        put("limitType", data.getLimitType());
        put("utilization", data.getUtilization());
        put("warningLevel", data.getWarningLevel());
        put("throttleLevel", data.getThrottleLevel());
        put("rejectLevel", data.getRejectLevel());
    }

    private void verify(PrismaReports.RiskLimitUtilization data) {
        checkArgument(data.hasClearer(), "Missing clearer in AMQP data");
        checkArgument(data.hasMember(), "Missing member in AMQP data");
        checkArgument(data.hasMaintainer(), "Missing maintainer in AMQP data");
        checkArgument(data.hasLimitType(), "Missing limit type in AMQP data");
        checkArgument(data.hasUtilization(), "Missing utilization in AMQP data");
        checkArgument(data.hasWarningLevel() ||
                      data.hasThrottleLevel() ||
                      data.hasRejectLevel(), "At least one of the levels " +
                                    "(warning, throttle or reject) has to be specified");
    }

    @Override
    public String getHistoryCollection() {
        return RiskLimitUtilizationModel.MONGO_HISTORY_COLLECTION;
    }

    @Override
    public String getLatestCollection() {
        return RiskLimitUtilizationModel.MONGO_LATEST_COLLECTION;
    }

    @Override
    public JsonObject getLatestQueryParams() {
        JsonObject queryParams = new JsonObject()
                .put("clearer", getString("clearer"))
                .put("member", getString("member"))
                .put("maintainer", getString("maintainer"))
                .put("limitType", getString("limitType"));
        return queryParams;
    }

    @Override
    public JsonObject getLatestUniqueIndex() {
        return new JsonObject()
                .put("clearer", 1)
                .put("member", 1)
                .put("maintainer", 1)
                .put("limitType", 1);
    }

}
