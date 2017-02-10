package com.deutscheboerse.risk.dave.model;

import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;

public class AccountMarginModelTest {

    @Test
    public void testGetAndSetValues() {
        AccountMarginModel accountMargin = new AccountMarginModel();
        accountMargin.setSnapshotID(5);
        accountMargin.setBusinessDate(20091215);
        accountMargin.setTimestamp(1486465721933L);
        accountMargin.setClearer("SFUCC");
        accountMargin.setMember("SFUFR");
        accountMargin.setAccount("A5");
        accountMargin.setMarginCurrency("EUR");
        accountMargin.setClearingCurrency("EUR");
        accountMargin.setPool("default");
        accountMargin.setMarginReqInMarginCurr(5.035485884371926E7);
        accountMargin.setMarginReqInCrlCurr(5.035485884371926E7);
        accountMargin.setUnadjustedMarginRequirement(5.035485884371926E7);
        accountMargin.setVariationPremiumPayment(0.0);

        Assert.assertEquals(5, accountMargin.getSnapshotID());
        Assert.assertEquals(20091215, accountMargin.getBusinessDate());
        Assert.assertEquals(new JsonObject().put("$date", "2017-02-07T11:08:41.933Z"), accountMargin.getTimestamp());
        Assert.assertEquals("SFUCC", accountMargin.getClearer());
        Assert.assertEquals("SFUFR", accountMargin.getMember());
        Assert.assertEquals("A5", accountMargin.getAccount());
        Assert.assertEquals("EUR", accountMargin.getMarginCurrency());
        Assert.assertEquals("EUR", accountMargin.getClearingCurrency());
        Assert.assertEquals("default", accountMargin.getPool());
        Assert.assertEquals(5.035485884371926E7, accountMargin.getMarginReqInMarginCurr(), 0);
        Assert.assertEquals(5.035485884371926E7, accountMargin.getMarginReqInCrlCurr(), 0);
        Assert.assertEquals(5.035485884371926E7, accountMargin.getUnadjustedMarginRequirement(), 0);
        Assert.assertEquals(0.0, accountMargin.getVariationPremiumPayment(), 0);
    }
}
