package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;

public class LiquiGroupSplitMarginModelTest {

    @Test
    public void testGetAndSetValues() {

        PrismaReports.PrismaHeader header = PrismaReports.PrismaHeader.newBuilder()
                .setId(15)
                .setBusinessDate(20091215)
                .setTimestamp(1487172422430L)
                .build();
        PrismaReports.LiquiGroupSplitMargin data = PrismaReports.LiquiGroupSplitMargin.newBuilder()
                .setKey(PrismaReports.LiquiGroupSplitMarginKey.newBuilder()
                        .setClearer("USJPM")
                        .setMember("USJPM")
                        .setAccount("PP")
                        .setLiquidationGroup("PFI02")
                        .setLiquidationGroupSplit("PFI02_HP2_T3-99999")
                        .setMarginCurrency("EUR"))
                .setPremiumMargin(0.0)
                .setMarketRisk(2.7548216040760565E8)
                .setLiquRisk(3.690967426538666E7)
                .setLongOptionCredit(0.0)
                .setVariationPremiumPayment(4.86621581017E8)
                .build();

        LiquiGroupSplitMarginModel liquiGroupSplitMarginModel = new LiquiGroupSplitMarginModel(header, data);

        JsonObject expected = new JsonObject()
                .put("snapshotID", 15)
                .put("businessDate", 20091215)
                .put("timestamp", new JsonObject().put("$date", "2017-02-15T15:27:02.43Z"))
                .put("clearer", "USJPM")
                .put("member", "USJPM")
                .put("account", "PP")
                .put("liquidationGroup", "PFI02")
                .put("liquidationGroupSplit", "PFI02_HP2_T3-99999")
                .put("marginCurrency", "EUR")
                .put("premiumMargin", 0.0)
                .put("marketRisk", 2.7548216040760565E8)
                .put("liquRisk", 3.690967426538666E7)
                .put("longOptionCredit", 0.0)
                .put("variationPremiumPayment", 4.86621581017E8);

        Assert.assertEquals(expected, new JsonObject(liquiGroupSplitMarginModel.getMap()));
    }
}
