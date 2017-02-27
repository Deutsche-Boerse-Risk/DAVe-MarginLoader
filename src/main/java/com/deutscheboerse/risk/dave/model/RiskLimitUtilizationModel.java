package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class RiskLimitUtilizationModel extends AbstractModel {
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
    public Collection<String> getKeys() {
        List<String> keys = new ArrayList();
        keys.add("clearer");
        keys.add("member");
        keys.add("maintainer");
        keys.add("limitType");
        return Collections.unmodifiableCollection(keys);
    }
}
