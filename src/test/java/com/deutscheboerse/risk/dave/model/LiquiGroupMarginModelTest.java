package com.deutscheboerse.risk.dave.model;

import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;

public class LiquiGroupMarginModelTest {

    @Test
    public void testGetAndSetValues() {
        LiquiGroupMarginModel liquiGroupMargin = new LiquiGroupMarginModel();
        liquiGroupMargin.setSnapshotID(5);
        liquiGroupMargin.setBusinessDate(20091215);
        liquiGroupMargin.setTimestamp(1486465721933L);
        liquiGroupMargin.setClearer("ABCFR");
        liquiGroupMargin.setMember("ABCFR");
        liquiGroupMargin.setAccount("PP");
        liquiGroupMargin.setMarginClass("ECC01");
        liquiGroupMargin.setMarginCurrency("EUR");
        liquiGroupMargin.setMarginGroup("");
        liquiGroupMargin.setPremiumMargin(135000.5);
        liquiGroupMargin.setCurrentLiquidatingMargin(0.0);
        liquiGroupMargin.setFuturesSpreadMargin(0.0);
        liquiGroupMargin.setAdditionalMargin(14914.841270178167);
        liquiGroupMargin.setUnadjustedMarginRequirement(149915.34127017818);
        liquiGroupMargin.setVariationPremiumPayment(0.0);

        Assert.assertEquals(5, liquiGroupMargin.getSnapshotID());
        Assert.assertEquals(20091215, liquiGroupMargin.getBusinessDate());
        Assert.assertEquals(new JsonObject().put("$date", "2017-02-07T11:08:41.933Z"), liquiGroupMargin.getTimestamp());
        Assert.assertEquals("ABCFR", liquiGroupMargin.getClearer());
        Assert.assertEquals("ABCFR", liquiGroupMargin.getMember());
        Assert.assertEquals("PP", liquiGroupMargin.getAccount());
        Assert.assertEquals("ECC01", liquiGroupMargin.getMarginClass());
        Assert.assertEquals("EUR", liquiGroupMargin.getMarginCurrency());
        Assert.assertEquals("", liquiGroupMargin.getMarginGroup());
        Assert.assertEquals(135000.5, liquiGroupMargin.getPremiumMargin(), 0);
        Assert.assertEquals(0.0, liquiGroupMargin.getCurrentLiquidatingMargin(), 0);
        Assert.assertEquals(0.0, liquiGroupMargin.getFuturesSpreadMargin(), 0);
        Assert.assertEquals(14914.841270178167, liquiGroupMargin.getAdditionalMargin(), 0);
        Assert.assertEquals(149915.34127017818, liquiGroupMargin.getUnadjustedMarginRequirement(), 0);
        Assert.assertEquals(0.0, liquiGroupMargin.getVariationPremiumPayment(), 0);
    }
}
