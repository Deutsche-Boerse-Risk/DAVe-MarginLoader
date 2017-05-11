package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;

public class LiquiGroupMarginModelTest {

    @Test
    public void testConversionFromPrisma() throws Exception {
        JsonObject json = DataHelper.getLastJsonFromFile("liquiGroupMargin", 1).orElseThrow(Exception::new);
        PrismaReports.PrismaHeader header = DataHelper.createPrismaHeaderFromJson(json);
        PrismaReports.LiquiGroupMargin data = DataHelper.createPrismaLiquiGroupMarginFromJson(json);

        LiquiGroupMarginModel modelFromPrisma = new LiquiGroupMarginModel(header, data);
        LiquiGroupMarginModel modelFromJson = DataHelper.createLiquiGroupMarginModelFromJson(json);

        Assert.assertEquals(modelFromJson, modelFromPrisma);
    }
}
