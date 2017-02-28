package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;

public class LiquiGroupMarginModelTest {

    @Test
    public void testGetAndSetValues() {
        PrismaReports.PrismaHeader header = PrismaReports.PrismaHeader.newBuilder()
                .setId(5)
                .setBusinessDate(20091215)
                .setTimestamp(1486465721933L)
                .build();
        PrismaReports.LiquiGroupMargin data = PrismaReports.LiquiGroupMargin.newBuilder()
                .setKey(PrismaReports.LiquiGroupMarginKey.newBuilder()
                        .setClearer("ABCFR")
                        .setMember("ABCFR")
                        .setAccount("PP")
                        .setMarginClass("ECC01")
                        .setMarginCurrency("EUR"))
                .setMarginGroup("")
                .setPremiumMargin(135000.5)
                .setCurrentLiquidatingMargin(0.0)
                .setFuturesSpreadMargin(0.0)
                .setAdditionalMargin(14914.841270178167)
                .setUnadjustedMarginRequirement(149915.34127017818)
                .setVariationPremiumPayment(0.0)
                .build();

        LiquiGroupMarginModel liquiGroupMarginModel = new LiquiGroupMarginModel(header, data);

        JsonObject expected = new JsonObject()
                .put("snapshotID", 5)
                .put("businessDate", 20091215)
                .put("timestamp", 1486465721933L)
                .put("clearer", "ABCFR")
                .put("member", "ABCFR")
                .put("account", "PP")
                .put("marginClass", "ECC01")
                .put("marginCurrency", "EUR")

                .put("marginGroup", "")
                .put("premiumMargin", 135000.5)
                .put("currentLiquidatingMargin", 0.0)
                .put("futuresSpreadMargin", 0.0)
                .put("additionalMargin", 14914.841270178167)
                .put("unadjustedMarginRequirement", 149915.34127017818)
                .put("variationPremiumPayment", 0.0);

        Assert.assertEquals(expected, liquiGroupMarginModel.toJson());
    }
}
