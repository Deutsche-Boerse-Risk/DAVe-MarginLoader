package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;

public class RiskLimitUtilizationTest {

    @Test
    public void testGetAndSetValues() throws Exception {
        JsonObject json = DataHelper.getLastJsonFromFile("riskLimitUtilization", 1).orElseThrow(Exception::new);
        PrismaReports.PrismaHeader header = DataHelper.createPrismaHeaderFromJson(json);
        PrismaReports.RiskLimitUtilization data = DataHelper.createRiskLimitUtilizationGPBFromJson(json);

        RiskLimitUtilizationModel modelFromGPB = new RiskLimitUtilizationModel(header, data);
        RiskLimitUtilizationModel modelFromJson = new RiskLimitUtilizationModel(json);

        Assert.assertEquals(json, modelFromGPB.toJson());
        Assert.assertEquals(json, modelFromJson.toJson());
    }
}
