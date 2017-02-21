package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;

public class PositionReportTest {

    @Test
    public void testGetAndSetValues() {

        PrismaReports.PrismaHeader header = PrismaReports.PrismaHeader.newBuilder()
                .setId(16)
                .setBusinessDate(20091215)
                .setTimestamp(1487677496396L)
                .build();
        PrismaReports.PositionReport data = PrismaReports.PositionReport.newBuilder()
                .setKey(PrismaReports.PositionReportKey.newBuilder()
                        .setClearer("BERFR")
                        .setMember("BERFR")
                        .setAccount("PP")
                        .setLiquidationGroup("PEQ01")
                        .setLiquidationGroupSplit("PEQ01_Basic")
                        .setProduct("ALV")
                        .setCallPut("P")
                        .setContractYear(2010)
                        .setContractMonth(2)
                        .setExpiryDay(0)
                        .setExercisePrice(170.0)
                        .setVersion("0")
                        .setFlexContractSymbol("")
                )
                .setNetQuantityLs(-1943.0)
                .setNetQuantityEa(0.0)
                .setClearingCurrency("EUR")
                .setMVar(22.539110869169235)
                .setCompVar(21.725328222930678)
                .setCompCorrelationBreak(0.8137826570693243)
                .setCompCompressionError(0.0060874443941714195)
                .setCompLiquidityAddOn(6.287057332082231)
                .setCompLongOptionCredit(-28.832255656476406)
                .setProductCurrency("EUR")
                .setVariationPremiumPayment(0.0)
                .setPremiumMargin(0.0)
                .setNormalizedDelta(0.04391331213559379)
                .setNormalizedGamma(-0.0021834196488993104)
                .setNormalizedVega(-0.002614335157912494)
                .setNormalizedRho(0.00008193269784691068)
                .setNormalizedTheta(0.0004722838437817084)
                .setUnderlying("ALV")
                .build();

        PositionReportModel poolMargin = new PositionReportModel(header, data);

        JsonObject expected = new JsonObject()
                .put("snapshotID", 16)
                .put("businessDate", 20091215)
                .put("timestamp", new JsonObject().put("$date", "2017-02-21T11:44:56.396Z"))
                .put("clearer", "BERFR")
                .put("member", "BERFR")
                .put("account", "PP")
                .put("liquidationGroup", "PEQ01")
                .put("liquidationGroupSplit", "PEQ01_Basic")
                .put("product", "ALV")
                .put("callPut", "P")
                .put("contractYear", 2010)
                .put("contractMonth", 2)
                .put("expiryDay", 0)
                .put("exercisePrice", 170)
                .put("version", "0")
                .put("flexContractSymbol", "")
                .put("netQuantityLs", -1943.0)
                .put("netQuantityEa", 0.0)
                .put("clearingCurrency", "EUR")
                .put("mVar", 22.539110869169235)
                .put("compVar", 21.725328222930678)
                .put("compCorrelationBreak", 0.8137826570693243)
                .put("compCompressionError", 0.0060874443941714195)
                .put("compLiquidityAddOn", 6.287057332082231)
                .put("compLongOptionCredit", -28.832255656476406)
                .put("productCurrency", "EUR")
                .put("variationPremiumPayment", 0.0)
                .put("premiumMargin", 0.0)
                .put("normalizedDelta", 0.04391331213559379)
                .put("normalizedGamma", -0.0021834196488993104)
                .put("normalizedVega", -0.002614335157912494)
                .put("normalizedRho", 0.00008193269784691068)
                .put("normalizedTheta", 0.0004722838437817084)
                .put("underlying", "ALV");

        Assert.assertEquals(expected, new JsonObject(poolMargin.getMap()));
    }
}
