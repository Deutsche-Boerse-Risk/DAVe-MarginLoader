package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

@DataObject
public class RiskLimitUtilizationModel extends AbstractModel {
    public RiskLimitUtilizationModel() {
        super();
    }

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

    @Override
    public Collection<String> getKeys() {
        List<String> keys = new ArrayList<>();
        keys.add("clearer");
        keys.add("member");
        keys.add("maintainer");
        keys.add("limitType");
        return Collections.unmodifiableCollection(keys);
    }
}
