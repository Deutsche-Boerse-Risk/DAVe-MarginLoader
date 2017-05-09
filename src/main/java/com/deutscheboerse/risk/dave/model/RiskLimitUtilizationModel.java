package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.RiskLimitUtilization;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

@DataObject
public class RiskLimitUtilizationModel extends AbstractModel<RiskLimitUtilization> {

    public RiskLimitUtilizationModel(JsonObject json) {
        this.mergeIn(json);
    }

    public RiskLimitUtilizationModel(PrismaReports.PrismaHeader header, PrismaReports.RiskLimitUtilization data) {
        super(header);

        verify(data);

        PrismaReports.RiskLimitUtilizationKey key = data.getKey();
        put("clearer", key.getClearer());
        put("member", key.getMember());
        put("maintainer", key.getMaintainer());
        put("limitType", key.getLimitType());
        put("utilization", data.getUtilization());
        if (data.hasWarningLevel()) {
            put("warningLevel", data.getWarningLevel());
        }
        if (data.hasThrottleLevel()) {
            put("throttleLevel", data.getThrottleLevel());
        }
        if (data.hasRejectLevel()) {
            put("rejectLevel", data.getRejectLevel());
        }
    }

    @Override
    public RiskLimitUtilization toGrpc() {
        RiskLimitUtilization.Builder builder = RiskLimitUtilization.newBuilder()
                .setSnapshotId(this.getInteger("snapshotID"))
                .setBusinessDate(this.getInteger("businessDate"))
                .setTimestamp(this.getLong("timestamp"))
                .setClearer(this.getString("clearer"))
                .setMember(this.getString("member"))
                .setMaintainer(this.getString("maintainer"))
                .setLimitType(this.getString("limitType"))
                .setUtilization(this.getDouble("utilization"));

        if (this.containsKey("warningLevel")) {
            builder.setWarningLevel(this.getDouble("warningLevel"));
        }
        if (this.containsKey("throttleLevel")) {
            builder.setThrottleLevel(this.getDouble("throttleLevel"));
        }
        if (this.containsKey("rejectLevel")) {
            builder.setRejectLevel(this.getDouble("rejectLevel"));
        }

        return builder.build();
    }

    private void verify(PrismaReports.RiskLimitUtilization data) {
        checkArgument(data.hasKey(), "Missing risk limit utilization key in AMQP data");
        checkArgument(data.getKey().hasClearer(), "Missing clearer in AMQP data");
        checkArgument(data.getKey().hasMember(), "Missing member in AMQP data");
        checkArgument(data.getKey().hasMaintainer(), "Missing maintainer in AMQP data");
        checkArgument(data.getKey().hasLimitType(), "Missing limit type in AMQP data");
        checkArgument(data.hasUtilization(), "Missing utilization in AMQP data");
        checkArgument(data.hasWarningLevel() ||
                      data.hasThrottleLevel() ||
                      data.hasRejectLevel(), "At least one of the levels " +
                                    "(warning, throttle or reject) has to be specified");
    }
}
