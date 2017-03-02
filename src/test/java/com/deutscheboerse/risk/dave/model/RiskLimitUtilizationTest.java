package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;

public class RiskLimitUtilizationTest {

    @Test
    public void testGetAndSetValues() {

        PrismaReports.PrismaHeader header = PrismaReports.PrismaHeader.newBuilder()
                .setId(16)
                .setBusinessDate(20091215)
                .setTimestamp(1487677496396L)
                .build();
        PrismaReports.RiskLimitUtilization data = PrismaReports.RiskLimitUtilization.newBuilder()
                .setKey(PrismaReports.RiskLimitUtilizationKey.newBuilder()
                    .setClearer("FULCC")
                    .setMember("MALFR")
                    .setMaintainer("MALFR")
                    .setLimitType("TMR"))
                .setUtilization(8862049569.447277)
                .setWarningLevel(1010020.0)
                .setThrottleLevel(0.0)
                .setRejectLevel(1010020.0)
                .build();

        RiskLimitUtilizationModel riskLimitUtilization = new RiskLimitUtilizationModel(header, data);

        JsonObject expected = new JsonObject()
                .put("snapshotID", 16)
                .put("businessDate", 20091215)
                .put("timestamp", 1487677496396L)
                .put("clearer", "FULCC")
                .put("member", "MALFR")
                .put("maintainer", "MALFR")
                .put("limitType", "TMR")
                .put("utilization", 8862049569.447277)
                .put("warningLevel", 1010020.0)
                .put("throttleLevel", 0.0)
                .put("rejectLevel", 1010020.0);

        Assert.assertEquals(expected, riskLimitUtilization.toJson());
    }
}
