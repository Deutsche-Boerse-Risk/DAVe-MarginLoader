package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;

public class PoolMarginModelTest {

    @Test
    public void testGetAndSetValues() {

        PrismaReports.PrismaHeader header = PrismaReports.PrismaHeader.newBuilder()
                .setId(10)
                .setBusinessDate(20091215)
                .setTimestamp(1487172422430L)
                .build();
        PrismaReports.PoolMargin data = PrismaReports.PoolMargin.newBuilder()
                .setKey(PrismaReports.PoolMarginKey.newBuilder()
                        .setClearer("USWFC")
                        .setPool("default")
                        .setMarginCurrency("JPY"))
                .setClrRptCurrency("EUR")
                .setRequiredMargin(738753550490.9158)
                .setCashCollateralAmount(-5129703568.382999)
                .setAdjustedSecurities(123456.45)
                .setAdjustedGuarantee(78594.98)
                .setOverUnderInMarginCurr(-743883254059.2988)
                .setOverUnderInClrRptCurr(-843883254059.2989)
                .setVariPremInMarginCurr(-6129703568.382999)
                .setAdjustedExchangeRate(1.2389)
                .setPoolOwner("FULCC")
                .build();

        PoolMarginModel poolMargin = new PoolMarginModel(header, data);

        JsonObject expected = new JsonObject()
                .put("snapshotID", 10)
                .put("businessDate", 20091215)
                .put("timestamp", 1487172422430L)
                .put("clearer", "USWFC")
                .put("pool", "default")
                .put("marginCurrency", "JPY")
                .put("clrRptCurrency", "EUR")
                .put("requiredMargin", 738753550490.9158)
                .put("cashCollateralAmount", -5129703568.382999)
                .put("adjustedSecurities", 123456.45)
                .put("adjustedGuarantee", 78594.98)
                .put("variPremInMarginCurr", -6.129703568382999E9)
                .put("overUnderInMarginCurr", -743883254059.2988)
                .put("overUnderInClrRptCurr", -843883254059.2989)
                .put("adjustedExchangeRate", 1.2389)
                .put("poolOwner", "FULCC");

        Assert.assertEquals(expected, poolMargin.toJson());
    }
}
