package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.RiskLimitUtilization;
import com.google.protobuf.InvalidProtocolBufferException;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

@DataObject
public class RiskLimitUtilizationModel implements Model<RiskLimitUtilization> {

    private final RiskLimitUtilization grpc;

    public RiskLimitUtilizationModel(JsonObject json) {
        verifyJson(json);
        try {
            this.grpc = RiskLimitUtilization.parseFrom(json.getBinary("grpc"));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public RiskLimitUtilizationModel(PrismaReports.PrismaHeader header, PrismaReports.RiskLimitUtilization data) {
        verifyPrismaHeader(header);
        verifyPrismaData(data);

        PrismaReports.RiskLimitUtilizationKey key = data.getKey();
        this.grpc = RiskLimitUtilization.newBuilder()
                .setSnapshotId(header.getId())
                .setBusinessDate(header.getBusinessDate())
                .setTimestamp(header.getTimestamp())
                .setClearer(key.getClearer())
                .setMember(key.getMember())
                .setMaintainer(key.getMaintainer())
                .setLimitType(key.getLimitType())
                .setUtilization(data.getUtilization())
                .setWarningLevel(data.getWarningLevel())
                .setThrottleLevel(data.getThrottleLevel())
                .setRejectLevel(data.getRejectLevel())
                .build();
    }

    @Override
    public JsonObject toJson() {
        return new JsonObject().put("grpc", this.grpc.toByteArray());
    }

    @Override
    public RiskLimitUtilization toGrpc() {
        return this.grpc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof RiskLimitUtilizationModel))
            return false;
        return this.grpc.equals(((RiskLimitUtilizationModel) o).grpc);
    }

    @Override
    public int hashCode() {
        return this.grpc.hashCode();
    }

    private void verifyPrismaData(PrismaReports.RiskLimitUtilization data) {
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
