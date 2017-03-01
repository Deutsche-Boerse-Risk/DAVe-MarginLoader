package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;

public class AccountMarginModelTest {

    @Test
    public void testGetAndSetValues() {
        PrismaReports.PrismaHeader header = PrismaReports.PrismaHeader.newBuilder()
                .setId(5)
                .setBusinessDate(20091215)
                .setTimestamp(1486465721933L)
                .build();
        PrismaReports.AccountMargin data = PrismaReports.AccountMargin.newBuilder()
                .setKey(PrismaReports.AccountMarginKey.newBuilder()
                        .setClearer("SFUCC")
                        .setMember("SFUFR")
                        .setAccount("A5")
                        .setMarginCurrency("EUR"))
                .setClearingCurrency("EUR")
                .setPool("default")
                .setMarginReqInMarginCurr(5.035485884371926E7)
                .setMarginReqInClrCurr(5.035485884371926E7)
                .setUnadjustedMarginRequirement(5.035485884371926E7)
                .setVariationPremiumPayment(0.0)
                .build();

        AccountMarginModel accountMarginModel = new AccountMarginModel(header, data);

        JsonObject expected = new JsonObject()
                .put("snapshotID", 5)
                .put("businessDate", 20091215)
                .put("timestamp", 1486465721933L)
                .put("clearer", "SFUCC")
                .put("member", "SFUFR")
                .put("account", "A5")
                .put("marginCurrency", "EUR")
                .put("clearingCurrency", "EUR")
                .put("pool", "default")
                .put("marginReqInMarginCurr", 5.035485884371926E7)
                .put("marginReqInCrlCurr", 5.035485884371926E7)
                .put("unadjustedMarginRequirement", 5.035485884371926E7)
                .put("variationPremiumPayment", 0.0);

        Assert.assertEquals(expected, accountMarginModel.toJson());
    }
}
