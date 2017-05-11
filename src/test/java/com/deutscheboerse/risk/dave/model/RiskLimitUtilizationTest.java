package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;

public class RiskLimitUtilizationTest {

    @Test
    public void testConversionFromPrisma() throws Exception {
        JsonObject json = DataHelper.getLastJsonFromFile("riskLimitUtilization", 1).orElseThrow(Exception::new);
        PrismaReports.PrismaHeader header = DataHelper.createPrismaHeaderFromJson(json);
        PrismaReports.RiskLimitUtilization data = DataHelper.createPrismaRiskLimitUtilizationFromJson(json);

        RiskLimitUtilizationModel modelFromPrisma = new RiskLimitUtilizationModel(header, data);
        RiskLimitUtilizationModel modelFromJson = DataHelper.createRiskLimitUtilizationModelFromJson(json);

        Assert.assertEquals(modelFromJson, modelFromPrisma);
    }
}
